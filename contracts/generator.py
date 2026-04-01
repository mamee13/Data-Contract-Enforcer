import argparse
import json
import os
import yaml
import pandas as pd
from datetime import datetime
from typing import Any, Optional

# Standard Bitol Schema key overrides for local data variations
KEY_OVERRIDES = {
    "week3-document-refinery-extractions": {
        "model": "extraction_model",
        "meta": "metadata"
    },
    "langsmith-traces": {
        "id": "trace_id",
        "name": "run_name"
    }
}

# Columns that are known primary/unique keys by name pattern
UNIQUE_PATTERNS = ("_id", "_hash", "_key")

# Max cardinality to treat a string column as an enum
ENUM_CARDINALITY_THRESHOLD = 40

# Column name suffixes/substrings that should NEVER get enum inference
# regardless of observed cardinality — these are identifiers, paths, timestamps, etc.
ENUM_BLOCKLIST_PATTERNS = ("_path", "_hash", "_url", "_at", "_id", "_key", "_ref", "_excerpt", "_text")

# Nested array paths to profile for specific contract IDs
NESTED_PROFILES: dict[str, list[dict[str, Any]]] = {
    "week3-document-refinery-extractions": [
        {
            "path": "extracted_facts[*].confidence",
            "array_key": "extracted_facts",
            "field": "confidence",
            "dtype": "float",
            # Semantic bounds override — do not use observed min/max
            "range_override": {"min": 0.0, "max": 1.0},
        },
        {
            "path": "extracted_facts[*].fact_id",
            "array_key": "extracted_facts",
            "field": "fact_id",
            "dtype": "string",
            "unique": True,
        },
        {
            "path": "entities[*].type",
            "array_key": "entities",
            "field": "type",
            "dtype": "string",
        },
    ]
}


class ContractGenerator:
    def __init__(self, source: str, contract_id: str, lineage_path: str, output_path: str):
        self.source = source
        self.contract_id = contract_id
        self.lineage_path = lineage_path
        self.output_path = output_path
        self.df: Optional[pd.DataFrame] = None
        self.records: list[dict] = []
        self.lineage_context: list[dict[str, str]] = []

    def load_data(self):
        """Loads JSONL records into a Pandas DataFrame."""
        with open(self.source, "r") as f:
            for line in f:
                if line.strip():
                    self.records.append(json.loads(line))
        self.df = pd.DataFrame(self.records)

    def load_lineage(self):
        """Extracts downstream nodes from the lineage graph as context."""
        if not self.lineage_path or not os.path.exists(self.lineage_path):
            return
        try:
            with open(self.lineage_path, "r") as f:
                lineage_data = json.loads(f.readline())
                self.lineage_context = [
                    {"id": node.get("node_id"), "type": node.get("type")}
                    for node in lineage_data.get("nodes", [])[:3]
                ]
        except Exception:
            self.lineage_context = [{"id": "unknown_downstream", "type": "PIPELINE"}]

    def _get_description(self, col: str) -> str:
        descriptions = {
            "intent_id": "Unique identifier for the intent record.",
            "text": "The raw input text for intent classification.",
            "predicted_intent": "The intent predicted by the model.",
            "confidence": "Confidence score of the prediction. Float 0.0–1.0.",
            "aggregate_id": "Unique identifier for the domain aggregate.",
            "occurred_at": "ISO 8601 timestamp of when the domain event occurred.",
            "recorded_at": "ISO 8601 timestamp of when the event was persisted. Must be >= occurred_at.",
            "payload": "Event-type-specific structured data content.",
            "event_id": "Primary key. UUIDv4. Globally unique across all events.",
            "event_type": "PascalCase event type name registered in the schema registry.",
            "aggregate_type": "PascalCase type of the domain aggregate.",
            "sequence_number": "Monotonically increasing integer per aggregate_id. No gaps or duplicates.",
            "schema_version": "Semver schema version of the event record format.",
            "doc_id": "Primary key. UUIDv4. Stable across re-extractions of the same source.",
            "source_path": "Absolute path or HTTPS URL of the source document.",
            "source_hash": "SHA-256 hex digest of the source file.",
            "extracted_facts": "Array of fact objects extracted from the document.",
            "entities": "Named entities identified in the document.",
            "extraction_model": "Model identifier used for extraction. Must match ^(claude|gpt)-.",
            "processing_time_ms": "Wall-clock extraction time in milliseconds. Must be positive.",
            "token_count": "Token usage object with integer fields input and output.",
            "extracted_at": "ISO 8601 timestamp of extraction completion.",
            "metadata": "Routing and tracing metadata object.",
        }
        return descriptions.get(col, f"Field: {col}.")

    def _infer_checks(self, col: str, mapped_name: str, dtype: str, series: pd.Series) -> list[dict]:
        """Infer contract checks from column name, type, and value distribution."""
        checks: list[dict[str, Any]] = []

        # Not null for every column
        checks.append({
            "check_id": f"not_null_{mapped_name}",
            "column_name": mapped_name,
            "check_type": "not_null",
            "severity": "CRITICAL"
        })

        # Unique inference: ID columns and hash columns
        if any(mapped_name.endswith(p) for p in UNIQUE_PATTERNS):
            non_null = series.dropna()
            # Skip unhashable types
            sample = non_null.iloc[0] if len(non_null) > 0 else None
            if isinstance(sample, (list, dict)):
                pass
            elif len(non_null) > 0 and non_null.nunique() == len(non_null):
                checks.append({
                    "check_id": f"unique_{mapped_name}",
                    "column_name": mapped_name,
                    "check_type": "unique",
                    "severity": "CRITICAL"
                })

        # Enum inference: low-cardinality string columns (skip unhashable types like list/dict)
        if "object" in dtype:
            non_null = series.dropna()
            # Skip columns whose values are lists or dicts (nested arrays/objects)
            sample = non_null.iloc[0] if len(non_null) > 0 else None
            if isinstance(sample, (list, dict)):
                return checks
            # Skip identifier/path/timestamp columns — low cardinality on small datasets
            # does not make them categorical
            if any(mapped_name.endswith(p) or mapped_name == p.lstrip("_")
                   for p in ENUM_BLOCKLIST_PATTERNS):
                return checks
            try:
                cardinality = non_null.nunique()
            except TypeError:
                return checks
            if 0 < cardinality <= ENUM_CARDINALITY_THRESHOLD:
                enum_values = sorted(non_null.unique().tolist())
                checks.append({
                    "check_id": f"enum_{mapped_name}",
                    "column_name": mapped_name,
                    "check_type": "enum",
                    "params": {"values": enum_values},
                    "severity": "CRITICAL" if cardinality <= 10 else "HIGH"
                })

        # Numeric range and drift
        if "float" in dtype or "int" in dtype:
            non_null = series.dropna()
            if not non_null.empty:
                mean_val = float(non_null.mean())
                std_val = float(non_null.std()) if len(non_null) > 1 else 0.0
                min_val = float(non_null.min())
                max_val = float(non_null.max())

                checks.append({
                    "check_id": f"range_{mapped_name}",
                    "column_name": mapped_name,
                    "check_type": "range",
                    "params": {"min": min_val, "max": max_val},
                    "severity": "WARN"
                })
                checks.append({
                    "check_id": f"mean_drift_{mapped_name}",
                    "column_name": mapped_name,
                    "check_type": "drift",
                    "params": {"baseline_mean": mean_val, "std_dev": std_val},
                    "severity": "WARN"
                })

        return checks

    def _infer_nested_checks(self) -> list[dict]:
        """Infer checks for known nested array fields."""
        checks: list[dict[str, Any]] = []
        nested_specs: list[dict[str, Any]] = NESTED_PROFILES.get(self.contract_id, [])

        for spec_raw in nested_specs:
            spec: dict[str, Any] = spec_raw
            array_key = spec["array_key"]
            field = spec["field"]
            path = spec["path"]

            values = []
            for record in self.records:
                for item in record.get(array_key, []):
                    val = item.get(field)
                    if val is not None:
                        values.append(val)

            if not values:
                continue

            # Unique check for nested IDs
            if spec.get("unique"):
                checks.append({
                    "check_id": f"unique_{path.replace('[*].', '_').replace('.', '_')}",
                    "column_name": path,
                    "check_type": "unique",
                    "severity": "CRITICAL"
                })

            # Range check for numeric nested fields
            if spec["dtype"] == "float":
                numeric = pd.Series(pd.to_numeric(values, errors="coerce")).dropna()
                if not numeric.empty:
                    mean_val = float(numeric.mean())
                    std_val = float(numeric.std()) if len(numeric) > 1 else 0.0
                    # Use semantic override if provided, otherwise use observed min/max
                    range_override = spec.get("range_override")
                    range_min = range_override["min"] if range_override else float(numeric.min())
                    range_max = range_override["max"] if range_override else float(numeric.max())
                    checks.append({
                        "check_id": f"range_{path.replace('[*].', '_').replace('.', '_')}",
                        "column_name": path,
                        "check_type": "range",
                        "params": {"min": range_min, "max": range_max},
                        "severity": "CRITICAL"
                    })
                    checks.append({
                        "check_id": f"mean_drift_{path.replace('[*].', '_').replace('.', '_')}",
                        "column_name": path,
                        "check_type": "drift",
                        "params": {"baseline_mean": mean_val, "std_dev": std_val},
                        "severity": "CRITICAL"
                    })

            # Enum check for low-cardinality string nested fields
            if spec["dtype"] == "string":
                series = pd.Series(values).dropna()
                cardinality = series.nunique()
                if 0 < cardinality <= ENUM_CARDINALITY_THRESHOLD:
                    checks.append({
                        "check_id": f"enum_{path.replace('[*].', '_').replace('.', '_')}",
                        "column_name": path,
                        "check_type": "enum",
                        "params": {"values": sorted(series.unique().tolist())},
                        "severity": "CRITICAL"
                    })

        return checks

    def generate_contract(self) -> None:
        self.load_data()
        self.load_lineage()

        if self.df is None or self.df.empty:
            print("No records found in source.")
            return

        columns: list[dict] = []
        checks: list[dict] = []
        overrides = KEY_OVERRIDES.get(self.contract_id, {})

        for col in self.df.columns:
            mapped_name = overrides.get(col, col)
            dtype = str(self.df[col].dtype)
            series = self.df[col]

            col_data_type = (
                "string" if "object" in dtype
                else "number" if ("float" in dtype or "int" in dtype)
                else "boolean"
            )

            columns.append({
                "name": mapped_name,
                "data_type": col_data_type,
                "description": self._get_description(mapped_name),
                "llm_annotations": ["ambiguous"] if col in ["meta", "payload", "metadata"] else []
            })

            checks.extend(self._infer_checks(col, mapped_name, dtype, series))

        # Add nested array checks for known schemas
        checks.extend(self._infer_nested_checks())

        # Add temporal constraint for event records
        if self.contract_id == "week5-event-records":
            checks.append({
                "check_id": "temporal_recorded_at_gte_occurred_at",
                "column_name": "recorded_at",
                "check_type": "temporal_gte",
                "params": {"reference_column": "occurred_at"},
                "severity": "CRITICAL"
            })
            checks.append({
                "check_id": "monotonic_sequence_number_per_aggregate",
                "column_name": "sequence_number",
                "check_type": "monotonic_per_group",
                "params": {"group_by": "aggregate_id"},
                "severity": "CRITICAL"
            })

        contract = {
            "bitol_version": "1.1.0",
            "id": self.contract_id,
            "name": self.contract_id.replace("-", " ").title(),
            "owner": "data-governance-team",
            "description": f"Generated contract for {self.contract_id}",
            "lineage": {
                "downstream": self.lineage_context
            },
            "columns": columns,
            "checks": checks
        }

        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        with open(self.output_path, "w") as f:
            yaml.dump(contract, f, sort_keys=False)

        # Save timestamped snapshot
        snapshot_dir = f"schema_snapshots/{self.contract_id}"
        os.makedirs(snapshot_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d%H%M%S%f")
        snapshot_path = f"{snapshot_dir}/snapshot_{ts}.yaml"
        with open(snapshot_path, "w") as f:
            yaml.dump(contract, f, sort_keys=False)

        # Extra artifacts
        if self.contract_id in ["week3-document-refinery-extractions", "week5-event-records"]:
            self.generate_dbt_schema(columns, checks)

        if self.contract_id == "week3-document-refinery-extractions":
            self.generate_prompt_input_schema(columns)

        print(f"Contract written to {self.output_path}")
        print(f"Snapshot written to {snapshot_path}")

    def generate_dbt_schema(self, columns: list[dict], checks: list[dict]) -> None:
        """Generate dbt schema.yml with meaningful tests inferred from contract checks."""
        dbt_path = self.output_path.replace(".yaml", "_dbt.yml")

        # Build a map of column -> list of dbt tests
        col_tests: dict[str, list] = {}
        for col in columns:
            col_tests[col["name"]] = ["not_null"]

        for check in checks:
            col = check.get("column_name", "")
            ctype = check.get("check_type", "")
            params = check.get("params", {})

            # Skip nested paths for top-level dbt model (handled in unnested models)
            if "[*]" in col:
                continue

            if ctype == "unique" and col in col_tests:
                col_tests[col].append("unique")
            elif ctype == "enum" and col in col_tests:
                col_tests[col].append({
                    "accepted_values": {"values": params.get("values", [])}
                })

        dbt_columns = []
        for col in columns:
            dbt_columns.append({
                "name": col["name"],
                "description": col["description"],
                "tests": col_tests.get(col["name"], ["not_null"])
            })

        dbt_schema = {
            "version": 2,
            "models": [{
                "name": self.contract_id,
                "description": f"dbt model for {self.contract_id}. Mirrors contract checks.",
                "columns": dbt_columns
            }]
        }

        with open(dbt_path, "w") as f:
            yaml.dump(dbt_schema, f, sort_keys=False)

        print(f"dbt schema written to {dbt_path}")

    def generate_prompt_input_schema(self, columns: list[dict]) -> None:
        prompt_dir = "generated_contracts/prompt_inputs"
        os.makedirs(prompt_dir, exist_ok=True)
        prompt_path = f"{prompt_dir}/week3_extraction_prompt_input.json"

        properties = {
            c["name"]: {
                "type": "string" if c["data_type"] == "string" else "number",
                "description": c["description"]
            }
            for c in columns
        }
        prompt_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": properties,
            "required": [c["name"] for c in columns if "id" in c["name"]]
        }
        with open(prompt_path, "w") as f:
            json.dump(prompt_schema, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Bitol Contract Generator")
    parser.add_argument("--source", required=True)
    parser.add_argument("--contract-id", default=None,
                        help="Contract ID (inferred from source filename if omitted)")
    parser.add_argument("--lineage", default="outputs/week4/lineage_snapshots.jsonl",
                        help="Path to lineage snapshot JSONL")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    if args.contract_id is None:
        stem = os.path.splitext(os.path.basename(args.source))[0]
        contract_id = stem.replace("_", "-")
    else:
        contract_id = args.contract_id

    output_path = args.output
    if os.path.isdir(output_path):
        output_path = os.path.join(output_path, f"{contract_id}.yaml")

    generator = ContractGenerator(args.source, contract_id, args.lineage, output_path)
    generator.generate_contract()


if __name__ == "__main__":
    main()
