import argparse
import json
import os
import yaml
import pandas as pd
from datetime import datetime
from typing import Optional

# Standard Bitol Schema key overrides for local data variations
KEY_OVERRIDES = {
    "week5-event-records": {
        "aggregate_id": "entity_id",
        "aggregate_type": "entity_type",
        "occurred_at": "timestamp"
    },
    "week3-document-refinery-extractions": {
        "model": "extraction_model",
        "meta": "metadata"
    },
    "langsmith-traces": {
        "id": "trace_id",
        "name": "run_name"
    }
}

class ContractGenerator:
    def __init__(self, source: str, contract_id: str, lineage_path: str, output_path: str):
        self.source = source
        self.contract_id = contract_id
        self.lineage_path = lineage_path
        self.output_path = output_path
        self.df: Optional[pd.DataFrame] = None
        self.lineage_context: list[dict[str, str]] = []

    def load_data(self):
        """Loads JSONL records into a Pandas DataFrame."""
        records = []
        with open(self.source, "r") as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
        self.df = pd.DataFrame(records)

    def load_lineage(self):
        """Extracts downstream nodes from the lineage graph as context."""
        if not self.lineage_path or not os.path.exists(self.lineage_path):
            return
        try:
            with open(self.lineage_path, "r") as f:
                lineage_data = json.loads(f.readline())
                # Just take the first few nodes as downstream context for demonstration
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
            "confidence": "The confidence score of the prediction (0.0 to 1.0).",
            "aggregate_id": "Unique identifier for the domain entity.",
            "occurred_at": "When the domain event actually occurred.",
            "payload": "The structured data content of the event."
        }
        return descriptions.get(col, f"Field representing {col}.")

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

            columns.append({
                "name": mapped_name,
                "data_type": "string" if "object" in dtype else "number" if ("float" in dtype or "int" in dtype) else "boolean",
                "description": self._get_description(col),
                "llm_annotations": ["ambiguous"] if col in ["meta", "payload", "metadata"] else []
            })

            # Structural check: Not Null
            checks.append({
                "check_id": f"not_null_{mapped_name}",
                "column_name": mapped_name,
                "check_type": "not_null",
                "severity": "CRITICAL"
            })

            # Statistical check: Range / Mean (if numeric)
            if "float" in dtype or "int" in dtype:
                series = self.df[col].dropna()
                if not series.empty:
                    mean_val = float(series.mean())
                    std_val = float(series.std()) if len(series) > 1 else 0
                    min_val = float(series.min())
                    max_val = float(series.max())

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

        # Save main YAML contract
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
            self.generate_dbt_schema(columns)

        if self.contract_id == "week3-document-refinery-extractions":
            self.generate_prompt_input_schema(columns)

    def generate_dbt_schema(self, columns: list[dict]) -> None:
        dbt_path = self.output_path.replace(".yaml", "_dbt.yml")
        dbt_schema = {
            "version": 2,
            "models": [{
                "name": self.contract_id,
                "columns": [{"name": c["name"], "description": c["description"], "tests": ["not_null"]} for c in columns]
            }]
        }
        with open(dbt_path, "w") as f:
            yaml.dump(dbt_schema, f, sort_keys=False)

    def generate_prompt_input_schema(self, columns: list[dict]) -> None:
        prompt_dir = "generated_contracts/prompt_inputs"
        os.makedirs(prompt_dir, exist_ok=True)
        prompt_path = f"{prompt_dir}/week3_extraction_prompt_input.json"

        properties = {c["name"]: {"type": "string" if c["data_type"] == "string" else "number", "description": c["description"]} for c in columns}
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
    parser.add_argument("--contract-id", required=True)
    parser.add_argument("--lineage", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    generator = ContractGenerator(args.source, args.contract_id, args.lineage, args.output)
    generator.generate_contract()

if __name__ == "__main__":
    main()
