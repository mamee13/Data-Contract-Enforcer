#!/usr/bin/env python3
"""
contracts/runner.py - ValidationRunner

Executes all contract checks on a dataset snapshot and produces a structured
validation report in JSON format.

Usage:
    python contracts/runner.py --contract generated_contracts/week3_extractions.yaml \
                              --data outputs/week3/extractions.jsonl \
                              --output validation_reports/week3_20260331.json
"""

import json
import sys
import argparse
import hashlib
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml
import pandas as pd


class ValidationRunner:
    def __init__(self, contract_path: str, data_path: str, output_path: str):
        self.contract_path = contract_path
        self.data_path = data_path
        self.output_path = output_path
        self.contract = None
        self.data: Optional[pd.DataFrame] = None
        self.records: list[dict] = []
        self.baselines: dict = self._load_baselines()
        self.results: list[dict] = []
        self.report_id = str(uuid.uuid4())

    def _load_baselines(self) -> dict:
        """Load statistical baselines if they exist."""
        baseline_path = "schema_snapshots/baselines.json"
        if Path(baseline_path).exists():
            with open(baseline_path) as f:
                data = json.load(f)
                return data.get("columns", {})
        return {}

    def _save_baselines(self, column_stats: dict):
        """Save statistical baselines after first run."""
        baseline_path = "schema_snapshots/baselines.json"

        with open(baseline_path, "w") as f:
            json.dump(
                {"written_at": datetime.utcnow().isoformat(), "columns": column_stats}, f, indent=2
            )

    def _compute_snapshot_id(self) -> str:
        """Compute SHA-256 of the input data file."""
        with open(self.data_path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()[:16]

    def load_contract(self):
        """Load the contract YAML file."""
        with open(self.contract_path) as f:
            self.contract = yaml.safe_load(f)

    def load_data(self):
        """Load JSONL data records."""
        self.records = []
        with open(self.data_path) as f:
            for line in f:
                if line.strip():
                    self.records.append(json.loads(line))

        if not self.records:
            raise ValueError(f"No records found in {self.data_path}")

        self.data = pd.DataFrame(self.records)

    def _extract_nested_values(self, path_pattern: str) -> list:
        """Extract values from nested structures using a path pattern."""
        values = []

        for record in self.records:
            if path_pattern == "extracted_facts[*].confidence":
                for fact in record.get("extracted_facts", []):
                    if "confidence" in fact:
                        values.append(fact["confidence"])

            elif path_pattern == "extracted_facts[*].fact_id":
                for fact in record.get("extracted_facts", []):
                    if "fact_id" in fact:
                        values.append(fact["fact_id"])

            elif path_pattern == "entities[*].entity_id":
                for entity in record.get("entities", []):
                    if "entity_id" in entity:
                        values.append(entity["entity_id"])

            elif path_pattern == "entities[*].type":
                for entity in record.get("entities", []):
                    if "type" in entity:
                        values.append(entity["type"])

            elif path_pattern in record:
                val = record.get(path_pattern)
                if val is not None:
                    if isinstance(val, list):
                        values.extend(val)
                    else:
                        values.append(val)

        return values

    def run_checks(self):
        """Run all checks defined in the contract."""
        if not self.contract or not self.records:
            raise RuntimeError("Must call load_contract() and load_data() first")
        assert self.data is not None

        contract_id = self.contract.get("id", "unknown")  # noqa: F841

        for check in self.contract.get("checks", []):
            check_id = check.get("check_id", "")
            column_name = check.get("column_name", "")
            check_type = check.get("check_type", "")
            severity = check.get("severity", "MEDIUM")
            params = check.get("params", {})

            result = {
                "check_id": check_id,
                "column_name": column_name,
                "check_type": check_type,
                "status": "PASS",
                "actual_value": None,
                "expected": None,
                "severity": severity,
                "records_failing": 0,
                "sample_failing": [],
                "message": "",
            }

            try:
                if check_type == "not_null":
                    self._check_not_null(column_name, result)
                elif check_type == "range":
                    self._check_range(column_name, params, result)
                elif check_type == "drift":
                    self._check_drift(column_name, params, result)
                elif check_type == "enum":
                    self._check_enum(column_name, params, result)
                elif check_type == "unique":
                    self._check_unique(column_name, result)
                elif check_type == "temporal_gte":
                    self._check_temporal_gte(column_name, params, result)
                elif check_type == "monotonic_per_group":
                    self._check_monotonic_per_group(column_name, params, result)
                else:
                    result["status"] = "ERROR"
                    result["message"] = f"Unknown check type: {check_type}"
            except Exception as e:
                result["status"] = "ERROR"
                result["message"] = f"Check failed: {str(e)}"

            self.results.append(result)

    def _check_not_null(self, column_name: str, result: dict):
        """Check that column has no null values."""
        null_count = 0
        total_count = len(self.records)

        if column_name in self.data.columns:
            null_count = self.data[column_name].isna().sum()
        else:
            nested_vals = self._extract_nested_values(column_name)
            if not nested_vals:
                result["status"] = "ERROR"
                result["message"] = f"Column {column_name} not found in data"
                return

        if null_count > 0:
            result["status"] = "FAIL"
            result["records_failing"] = int(null_count)
            result["message"] = f"{null_count} of {total_count} records have null {column_name}"
            result["actual_value"] = f"null_count={null_count}"
        else:
            result["actual_value"] = "null_count=0"

        result["expected"] = "no null values"

    def _check_range(self, column_name: str, params: dict, result: dict):
        """Check that numeric values are within expected range."""
        min_val = params.get("min")
        max_val = params.get("max")

        if column_name in self.data.columns:
            col_data = pd.to_numeric(self.data[column_name], errors="coerce").dropna()
        else:
            col_data = pd.to_numeric(self._extract_nested_values(column_name), errors="coerce")

        if len(col_data) == 0:
            result["status"] = "ERROR"
            result["message"] = f"No numeric data for {column_name}"
            return

        actual_min = float(col_data.min())
        actual_max = float(col_data.max())

        failures = col_data[(col_data < min_val) | (col_data > max_val)]
        failures_list = failures.tolist() if hasattr(failures, "tolist") else list(failures)

        if len(failures_list) > 0:
            result["status"] = "FAIL"
            result["records_failing"] = len(failures_list)
            result["sample_failing"] = failures_list[:5]
            result["message"] = f"Values outside range [{min_val}, {max_val}]"
            result["actual_value"] = f"min={actual_min}, max={actual_max}"
        else:
            result["actual_value"] = f"min={actual_min}, max={actual_max}"

        result["expected"] = f"min>={min_val}, max<={max_val}"

    def _check_drift(self, column_name: str, params: dict, result: dict):
        """Check for statistical drift from baseline."""
        if column_name in self.data.columns:
            col_data = pd.to_numeric(self.data[column_name], errors="coerce").dropna()
        else:
            col_data = pd.to_numeric(self._extract_nested_values(column_name), errors="coerce")

        if len(col_data) == 0:
            result["status"] = "ERROR"
            result["message"] = f"No numeric data for {column_name}"
            return

        current_mean = float(col_data.mean())
        current_std = float(col_data.std()) if len(col_data) > 1 else 0.0

        baseline_mean = params.get("baseline_mean")
        baseline_std = params.get("std_dev")

        if baseline_mean is None or column_name not in self.baselines:
            result["status"] = "WARN"
            result["message"] = (
                f"No baseline available for {column_name}, running baseline established"
            )
            result["actual_value"] = f"mean={current_mean}, std={current_std}"
            result["expected"] = "baseline will be saved on first run"

            column_stats = dict(self.baselines)
            column_stats[column_name] = {
                "mean": current_mean,
                "stddev": current_std,
                "min": float(col_data.min()),
                "max": float(col_data.max()),
            }
            self._save_baselines(column_stats)
            return

        z_score = abs(current_mean - baseline_mean) / max(baseline_std, 1e-9)
        z_score = round(z_score, 2)

        result["actual_value"] = f"mean={current_mean}, z_score={z_score}"

        if z_score > 3:
            result["status"] = "FAIL"
            result["message"] = f"Mean drifted {z_score} stddev from baseline"
        elif z_score > 2:
            result["status"] = "WARN"
            result["message"] = f"Mean within warning range ({z_score} stddev)"
        else:
            result["message"] = f"Within acceptable range ({z_score} stddev)"

        result["expected"] = "drift < 2 stddev"

    def _check_enum(self, column_name: str, params: dict, result: dict):
        """Check that values are from allowed enum."""
        allowed_values = params.get("values", [])

        if column_name in self.data.columns:
            col_data = self.data[column_name].dropna()
        else:
            col_data = pd.Series(self._extract_nested_values(column_name))

        if len(col_data) == 0:
            result["status"] = "ERROR"
            result["message"] = f"No data for {column_name}"
            return

        invalid = col_data[~col_data.isin(allowed_values)]

        if len(invalid) > 0:
            result["status"] = "FAIL"
            result["records_failing"] = len(invalid)
            result["sample_failing"] = invalid.head(5).tolist()
            result["message"] = f"Values not in enum: {set(invalid.tolist())}"
            result["actual_value"] = f"unique_invalid={invalid.nunique()}"
        else:
            result["actual_value"] = "all values valid"

        result["expected"] = f"values in {allowed_values}"

    def _check_unique(self, column_name: str, result: dict):
        """Check that column values are unique."""
        if column_name in self.data.columns:
            col_data = self.data[column_name].dropna()
        else:
            col_data = pd.Series(self._extract_nested_values(column_name))

        total_count = len(col_data)
        unique_count = col_data.nunique()

        if unique_count < total_count:
            result["status"] = "FAIL"
            result["records_failing"] = total_count - unique_count
            result["message"] = f"Duplicates found: {total_count - unique_count}"
            result["actual_value"] = f"unique={unique_count}, total={total_count}"
        else:
            result["actual_value"] = f"unique={unique_count}"

        result["expected"] = "all unique"

    def _check_temporal_gte(self, column_name: str, params: dict, result: dict):
        """Check that column value >= reference_column value for every record."""
        reference_column = params.get("reference_column")
        if not reference_column:
            result["status"] = "ERROR"
            result["message"] = "temporal_gte check requires params.reference_column"
            return

        if column_name not in self.data.columns or reference_column not in self.data.columns:
            result["status"] = "ERROR"
            result["message"] = f"Column {column_name} or {reference_column} not found in data"
            return

        # Compare as strings — ISO 8601 timestamps sort lexicographically
        violations = self.data[self.data[column_name] < self.data[reference_column]]
        failing_count = len(violations)

        if failing_count > 0:
            result["status"] = "FAIL"
            result["records_failing"] = failing_count
            result["sample_failing"] = violations[[column_name, reference_column]].head(5).to_dict("records")
            result["message"] = f"{failing_count} records have {column_name} < {reference_column}"
            result["actual_value"] = f"violations={failing_count}"
        else:
            result["actual_value"] = "all records pass"

        result["expected"] = f"{column_name} >= {reference_column}"

    def _check_monotonic_per_group(self, column_name: str, params: dict, result: dict):
        """Check that column is monotonically increasing (no gaps, no duplicates) per group."""
        group_by = params.get("group_by")
        if not group_by:
            result["status"] = "ERROR"
            result["message"] = "monotonic_per_group check requires params.group_by"
            return

        if column_name not in self.data.columns or group_by not in self.data.columns:
            result["status"] = "ERROR"
            result["message"] = f"Column {column_name} or {group_by} not found in data"
            return

        failing_groups = []
        total_violations = 0

        for group_val, group_df in self.data.groupby(group_by):
            seqs = group_df[column_name].dropna().sort_values().tolist()
            # Check for duplicates
            if len(seqs) != len(set(seqs)):
                failing_groups.append(str(group_val))
                total_violations += len(seqs) - len(set(seqs))
                continue
            # Check for gaps (sequence should be consecutive integers starting at min)
            if seqs:
                expected = list(range(int(min(seqs)), int(min(seqs)) + len(seqs)))
                if [int(s) for s in seqs] != expected:
                    failing_groups.append(str(group_val))
                    total_violations += 1

        if failing_groups:
            result["status"] = "FAIL"
            result["records_failing"] = total_violations
            result["sample_failing"] = failing_groups[:5]
            result["message"] = (
                f"{len(failing_groups)} aggregates have non-monotonic {column_name} "
                f"(duplicates or gaps)"
            )
            result["actual_value"] = f"failing_groups={len(failing_groups)}"
        else:
            result["actual_value"] = "all groups monotonic"

        result["expected"] = f"{column_name} monotonically increasing per {group_by}"

    def generate_report(self) -> dict:
        """Generate the final validation report."""
        passed = sum(1 for r in self.results if r["status"] == "PASS")
        failed = sum(1 for r in self.results if r["status"] == "FAIL")
        warned = sum(1 for r in self.results if r["status"] == "WARN")
        errored = sum(1 for r in self.results if r["status"] == "ERROR")

        assert self.contract is not None
        report = {
            "report_id": self.report_id,
            "contract_id": self.contract.get("id", "unknown"),
            "snapshot_id": self._compute_snapshot_id(),
            "run_timestamp": datetime.utcnow().isoformat(),
            "total_checks": len(self.results),
            "passed": passed,
            "failed": failed,
            "warned": warned,
            "errored": errored,
            "results": self.results,
        }

        return report

    def run(self):
        """Execute the full validation pipeline."""
        print(f"Loading contract from {self.contract_path}...")
        self.load_contract()

        print(f"Loading data from {self.data_path}...")
        self.load_data()

        print(f"Running {len(self.contract.get('checks', []))} checks...")
        self.run_checks()

        report = self.generate_report()

        print(f"Writing report to {self.output_path}...")
        Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(self.output_path, "w") as f:
            json.dump(report, f, indent=2)

        print(f"\n{'=' * 60}")
        print("VALIDATION SUMMARY")
        print(f"{'=' * 60}")
        print(f"Total checks:  {report['total_checks']}")
        print(f"Passed:        {report['passed']}")
        print(f"Failed:        {report['failed']}")
        print(f"Warned:        {report['warned']}")
        print(f"Errored:       {report['errored']}")
        print(f"{'=' * 60}")

        return report


def main():
    parser = argparse.ArgumentParser(description="Run contract validation checks")
    parser.add_argument("--contract", required=True, help="Path to contract YAML file")
    parser.add_argument("--data", required=True, help="Path to data JSONL file")
    parser.add_argument("--output", required=True, help="Path to output JSON report")

    args = parser.parse_args()

    runner = ValidationRunner(args.contract, args.data, args.output)
    report = runner.run()

    failed_count = report["failed"]
    sys.exit(1 if failed_count > 0 else 0)


if __name__ == "__main__":
    main()
