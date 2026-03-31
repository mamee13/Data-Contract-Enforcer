#!/usr/bin/env python3
"""
contracts/attributor.py - ViolationAttributor

Traces contract violations back to their source using lineage traversal
and git blame integration.

Usage:
    python contracts/attributor.py --violation validation_reports/violated_run.json \
                                   --lineage outputs/week4/lineage_snapshots.jsonl \
                                   --contract generated_contracts/week3_extractions.yaml \
                                   --output violation_log/violations.jsonl
"""

import json
import argparse
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml


class ViolationAttributor:
    def __init__(
        self, violation_path: str, lineage_path: str, contract_path: str, output_path: str
    ):
        self.violation_path = violation_path
        self.lineage_path = lineage_path
        self.contract_path = contract_path
        self.output_path = output_path
        self.violation_report: Optional[dict] = None
        self.lineage_snapshot: Optional[dict] = None
        self.contract: Optional[dict] = None
        self.existing_violations: list[dict] = []

    def load_violation_report(self):
        """Load the validation report containing failures."""
        with open(self.violation_path) as f:
            self.violation_report = json.load(f)

    def load_lineage(self):
        """Load the latest lineage snapshot."""
        with open(self.lineage_path) as f:
            lines = f.readlines()
            if lines:
                self.lineage_snapshot = json.loads(lines[-1])

    def load_contract(self):
        """Load the contract YAML for blast radius computation."""
        with open(self.contract_path) as f:
            self.contract = yaml.safe_load(f)

    def load_existing_violations(self):
        """Load existing violations from the output file."""
        output_file = Path(self.output_path)
        if output_file.exists():
            with open(output_file) as f:
                for line in f:
                    if line.strip() and not line.startswith("#"):
                        try:
                            self.existing_violations.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass

    def find_upstream_files(self, failing_system: str) -> list[dict]:
        """Find upstream files that produce the failing data using lineage graph."""
        candidates: list[dict] = []

        if not self.lineage_snapshot:
            return candidates

        nodes = self.lineage_snapshot.get("nodes", [])

        for node in nodes:
            node_id = node.get("node_id", "")
            node_type = node.get("type", "")
            metadata = node.get("metadata", {})

            if failing_system.lower() in node_id.lower() and node_type == "FILE":
                candidates.append(
                    {"node_id": node_id, "path": metadata.get("path", node_id), "type": node_type}
                )

        return candidates

    def get_git_commits(self, file_path: str, days: int = 14) -> list[dict]:
        """Get recent git commits for a file."""
        commits: list[dict] = []

        if not Path(file_path).exists():
            return commits

        try:
            cmd = [
                "git",
                "log",
                "--follow",
                f"--since={days} days ago",
                "--format=%H|%an|%ae|%ai|%s",
                "--",
                file_path,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(file_path).parent)

            for line in result.stdout.strip().split("\n"):
                if "|" in line:
                    parts = line.split("|", 4)
                    if len(parts) >= 4:
                        commits.append(
                            {
                                "commit_hash": parts[0],
                                "author": parts[1],
                                "email": parts[2] if len(parts) > 2 else "",
                                "commit_timestamp": parts[3].strip() if len(parts) > 3 else "",
                                "commit_message": parts[4] if len(parts) > 4 else "",
                            }
                        )
        except Exception as e:
            print(f"Warning: Could not get git commits for {file_path}: {e}")

        return commits

    def compute_confidence_score(
        self, commit: dict, lineage_distance: int, violation_timestamp: str
    ) -> float:
        """Compute confidence score for a blame candidate.

        Formula: 1.0 - (days_since_commit * 0.1) - (lineage_hops * 0.2)
        """
        try:
            commit_time = datetime.fromisoformat(
                commit["commit_timestamp"].replace("Z", "+00:00").replace(" ", "+")
            )
            if isinstance(violation_timestamp, str):
                violation_time = datetime.fromisoformat(violation_timestamp.replace("Z", "+00:00"))
            else:
                violation_time = datetime.now(timezone.utc)

            days_diff = abs((violation_time - commit_time).days)
        except Exception:
            days_diff = 0

        score = max(0.0, 1.0 - (days_diff * 0.1) - (lineage_distance * 0.2))
        return round(score, 3)

    def compute_blast_radius(self) -> dict:
        """Compute blast radius from contract lineage.downstream."""
        assert self.contract is not None
        downstream = self.contract.get("lineage", {}).get("downstream", [])

        affected_nodes = [d.get("id", "") for d in downstream]
        affected_pipelines = [
            d.get("id", "") for d in downstream if "pipeline" in d.get("id", "").lower()
        ]

        return {
            "affected_nodes": affected_nodes,
            "affected_pipelines": affected_pipelines
            if affected_pipelines
            else ["extraction-pipeline"],
            "estimated_records": 0,
        }

    def attribute_violation(self, check_result: dict) -> dict:
        """Attribute a single violation to its source."""
        assert self.violation_report is not None
        check_id = check_result.get("check_id", "")
        column_name = check_result.get("column_name", "")

        failing_system = (
            column_name.split(".")[0] if "." in column_name else column_name.split("[")[0]
        )

        if "*" in failing_system:
            failing_system = failing_system.split("*")[0]

        upstream_files = self.find_upstream_files(failing_system)

        blame_chain: list[dict] = []

        for i, file_info in enumerate(upstream_files[:5]):
            file_path = file_info.get("path", "")

            if file_path and Path(file_path).exists():
                commits = self.get_git_commits(file_path)

                for commit in commits[:3]:
                    confidence = self.compute_confidence_score(
                        commit,
                        lineage_distance=i,
                        violation_timestamp=self.violation_report.get("run_timestamp", ""),
                    )

                    blame_chain.append(
                        {
                            "rank": len(blame_chain) + 1,
                            "file_path": file_path,
                            "commit_hash": commit.get("commit_hash", "unknown"),
                            "author": commit.get("author", "unknown"),
                            "commit_timestamp": commit.get("commit_timestamp", ""),
                            "commit_message": commit.get("commit_message", ""),
                            "confidence_score": confidence,
                        }
                    )

        if not blame_chain:
            blame_chain = [
                {
                    "rank": 1,
                    "file_path": upstream_files[0].get("path", "") if upstream_files else "unknown",
                    "commit_hash": "unknown",
                    "author": "unknown",
                    "commit_timestamp": "",
                    "commit_message": "Could not trace to git history",
                    "confidence_score": 0.0,
                }
            ]

        blame_chain = sorted(blame_chain, key=lambda x: x["confidence_score"], reverse=True)[:5]
        for i, candidate in enumerate(blame_chain):
            candidate["rank"] = i + 1

        blast_radius = self.compute_blast_radius()
        blast_radius["estimated_records"] = check_result.get("records_failing", 0)

        return {
            "violation_id": str(uuid.uuid4()),
            "check_id": check_id,
            "detected_at": self.violation_report.get(
                "run_timestamp", datetime.utcnow().isoformat()
            ),
            "blame_chain": blame_chain,
            "blast_radius": blast_radius,
        }

    def run(self):
        """Execute the attribution pipeline."""
        print(f"Loading violation report from {self.violation_path}...")
        self.load_violation_report()

        print(f"Loading lineage from {self.lineage_path}...")
        self.load_lineage()

        print(f"Loading contract from {self.contract_path}...")
        self.load_contract()

        print("Loading existing violations...")
        self.load_existing_violations()

        failed_checks = [
            r for r in self.violation_report.get("results", []) if r.get("status") == "FAIL"
        ]

        print(f"Attributing {len(failed_checks)} failed checks...")

        new_violations = []

        for check in failed_checks:
            violation = self.attribute_violation(check)
            new_violations.append(violation)

            print(f"  - {check.get('check_id')}: {len(violation['blame_chain'])} candidates")

        all_violations = self.existing_violations + new_violations

        Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)

        comment_line = "# VIOLATION LOG - Do not edit manually\n"

        with open(self.output_path, "w") as f:
            f.write(comment_line)
            for v in all_violations:
                f.write(json.dumps(v) + "\n")

        print(f"\n{'=' * 60}")
        print("ATTRIBUTION SUMMARY")
        print(f"{'=' * 60}")
        print(f"Total violations: {len(all_violations)}")
        print(f"New attributed:   {len(new_violations)}")
        print(f"Output:           {self.output_path}")
        print(f"{'=' * 60}")

        return all_violations


def main():
    parser = argparse.ArgumentParser(description="Attribute violations to their source")
    parser.add_argument("--violation", required=True, help="Path to validation report JSON")
    parser.add_argument("--lineage", required=True, help="Path to lineage snapshot JSONL")
    parser.add_argument("--contract", required=True, help="Path to contract YAML")
    parser.add_argument("--output", required=True, help="Path to output violations JSONL")

    args = parser.parse_args()

    attributor = ViolationAttributor(args.violation, args.lineage, args.contract, args.output)

    attributor.run()


if __name__ == "__main__":
    main()
