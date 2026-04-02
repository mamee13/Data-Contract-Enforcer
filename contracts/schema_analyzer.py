#!/usr/bin/env python3
"""
contracts/schema_analyzer.py - SchemaEvolutionAnalyzer

Diffs schema snapshots and classifies changes using the taxonomy.

Usage:
    python contracts/schema_analyzer.py --contract-id week3-document-refinery-extractions \
                                        --output validation_reports/schema_evolution.json
"""

import json
import sys
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml


class SchemaEvolutionAnalyzer:
    def __init__(self, contract_id: str, output_path: str):
        self.contract_id = contract_id
        self.output_path = output_path
        self.snapshots: list[dict] = []

    def load_snapshots(self):
        """Load all timestamped snapshots for the contract."""
        snapshot_dir = Path("schema_snapshots") / self.contract_id

        if not snapshot_dir.exists():
            print(f"Warning: No snapshots found for {self.contract_id}")
            return

        snapshot_files = sorted(snapshot_dir.glob("*.yaml"))

        for sf in snapshot_files:
            with open(sf) as f:
                self.snapshots.append(
                    {"path": str(sf), "timestamp": sf.stem, "data": yaml.safe_load(f)}
                )

        print(f"Loaded {len(self.snapshots)} snapshots")

    def classify_change(
        self, field_name: str, old_clause: Optional[dict], new_clause: Optional[dict]
    ) -> dict:
        """Classify a schema change using the taxonomy."""
        if old_clause is None and new_clause is not None:
            if new_clause.get("required", False):
                return {
                    "classification": "BREAKING",
                    "type": "ADD_NON_NULLABLE_COLUMN",
                    "description": f"Added required column {field_name}",
                    "action": "Coordinate with all producers",
                }
            return {
                "classification": "COMPATIBLE",
                "type": "ADD_NULLABLE_COLUMN",
                "description": f"Added optional column {field_name}",
                "action": "None - consumers can ignore",
            }

        if old_clause is not None and new_clause is None:
            return {
                "classification": "BREAKING",
                "type": "REMOVE_COLUMN",
                "description": f"Removed column {field_name}",
                "action": "Deprecation period mandatory",
            }

        if old_clause.get("type") != new_clause.get("type"):
            return {
                "classification": "BREAKING",
                "type": "TYPE_CHANGE",
                "description": f"Type changed from {old_clause.get('type')} to {new_clause.get('type')}",
                "action": "Requires migration plan",
            }

        if old_clause.get("maximum") != new_clause.get("maximum"):
            old_max = old_clause.get("maximum", "none")
            new_max = new_clause.get("maximum", "none")
            return {
                "classification": "BREAKING",
                "type": "RANGE_CHANGE",
                "description": f"Range maximum changed from {old_max} to {new_max}",
                "action": "May break downstream consumers",
            }

        old_enum = set(old_clause.get("enum", []))
        new_enum = set(new_clause.get("enum", []))

        if old_enum != new_enum:
            removed = old_enum - new_enum
            added = new_enum - old_enum
            if removed:
                return {
                    "classification": "BREAKING",
                    "type": "ENUM_VALUES_REMOVED",
                    "description": f"Enum values removed: {removed}",
                    "action": "Coordinate with consumers",
                }
            return {
                "classification": "COMPATIBLE",
                "type": "ENUM_VALUES_ADDED",
                "description": f"Enum values added: {added}",
                "action": "Notify consumers",
            }

        return {
            "classification": "COMPATIBLE",
            "type": "NO_CHANGE",
            "description": "No material change",
            "action": "None",
        }

    def classify_check_change(self, check_id: str, old_check: dict, new_check: dict) -> Optional[dict]:
        """Classify a change in a statistical check clause."""
        old_params = old_check.get("params", {})
        new_params = new_check.get("params", {})

        # Detect drift baseline_mean shift > 10x (scale change)
        old_mean = old_params.get("baseline_mean")
        new_mean = new_params.get("baseline_mean")
        if old_mean is not None and new_mean is not None and old_mean != new_mean:
            ratio = new_mean / old_mean if old_mean != 0 else float("inf")
            if ratio > 5 or ratio < 0.2:
                return {
                    "classification": "BREAKING",
                    "type": "STAT_BASELINE_SCALE_CHANGE",
                    "field": check_id,
                    "description": (
                        f"Statistical baseline mean changed from {round(old_mean, 4)} "
                        f"to {round(new_mean, 4)} (ratio {round(ratio, 2)}x) — "
                        "indicates a scale change in the underlying data"
                    ),
                    "action": "Investigate data pipeline for scale/unit change",
                }

        # Detect range max change
        old_max = old_params.get("max")
        new_max = new_params.get("max")
        if old_max is not None and new_max is not None and old_max != new_max:
            return {
                "classification": "BREAKING",
                "type": "RANGE_CHANGE",
                "field": check_id,
                "description": f"Range max changed from {old_max} to {new_max}",
                "action": "Update downstream validation logic",
            }

        return None

    def diff_snapshots(self) -> dict:
        """Diff two consecutive snapshots (columns + statistical checks)."""
        if len(self.snapshots) < 2:
            return {
                "changes": [],
                "compatibility_verdict": "UNKNOWN",
                "message": "Need at least 2 snapshots to diff",
            }

        old_snapshot = self.snapshots[-2]["data"]
        new_snapshot = self.snapshots[-1]["data"]

        changes = []

        # --- Column-level diff ---
        old_columns = {c["name"]: c for c in old_snapshot.get("columns", [])}
        new_columns = {c["name"]: c for c in new_snapshot.get("columns", [])}

        for field in set(old_columns.keys()) | set(new_columns.keys()):
            change = self.classify_change(field, old_columns.get(field), new_columns.get(field))
            change["field"] = field
            changes.append(change)

        # --- Check-level diff (statistical baselines, ranges) ---
        old_checks = {c["check_id"]: c for c in old_snapshot.get("checks", [])}
        new_checks = {c["check_id"]: c for c in new_snapshot.get("checks", [])}

        for check_id in set(old_checks.keys()) | set(new_checks.keys()):
            if check_id in old_checks and check_id in new_checks:
                check_change = self.classify_check_change(
                    check_id, old_checks[check_id], new_checks[check_id]
                )
                if check_change:
                    changes.append(check_change)
            elif check_id not in old_checks:
                changes.append({
                    "classification": "COMPATIBLE",
                    "type": "CHECK_ADDED",
                    "field": check_id,
                    "description": f"New check added: {check_id}",
                    "action": "None",
                })
            else:
                changes.append({
                    "classification": "BREAKING",
                    "type": "CHECK_REMOVED",
                    "field": check_id,
                    "description": f"Check removed: {check_id}",
                    "action": "Verify intentional removal",
                })

        breaking_changes = [c for c in changes if c["classification"] == "BREAKING"]

        if breaking_changes:
            compatibility = "BREAKING"
            verdict = f"{len(breaking_changes)} breaking change(s) detected"
        else:
            compatibility = "COMPATIBLE"
            verdict = "All changes are backward compatible"

        return {
            "old_snapshot": self.snapshots[-2]["timestamp"],
            "new_snapshot": self.snapshots[-1]["timestamp"],
            "changes": changes,
            "breaking_changes": breaking_changes,
            "compatibility_verdict": compatibility,
            "verdict_message": verdict,
        }

    def generate_migration_report(self, diff_result: dict) -> dict:
        """Generate migration impact report."""
        blast_radius = {
            "affected_contracts": [self.contract_id],
            "affected_consumers": ["week4-cartographer", "week5-event-sourcing"],
            "estimated_impact": "HIGH"
            if diff_result["compatibility_verdict"] == "BREAKING"
            else "LOW",
        }

        migration_checklist = []

        for change in diff_result.get("breaking_changes", []):
            field = change.get("field", "")
            change_type = change.get("type", "")

            if change_type == "ADD_NON_NULLABLE_COLUMN":
                migration_checklist.append(
                    {
                        "step": f"Add default value for {field}",
                        "priority": "HIGH",
                        "deadline": "Before next deployment",
                    }
                )
            elif change_type == "REMOVE_COLUMN":
                migration_checklist.append(
                    {
                        "step": f"Review and update consumers of {field}",
                        "priority": "HIGH",
                        "deadline": "Within 2 sprints",
                    }
                )
            elif change_type == "TYPE_CHANGE":
                migration_checklist.append(
                    {
                        "step": f"Update data transformation for {field}",
                        "priority": "MEDIUM",
                        "deadline": "Before schema promotion",
                    }
                )
            elif change_type == "RANGE_CHANGE":
                migration_checklist.append(
                    {
                        "step": f"Update validation logic for {field}",
                        "priority": "HIGH",
                        "deadline": "Immediate",
                    }
                )
            elif change_type == "STAT_BASELINE_SCALE_CHANGE":
                migration_checklist.append(
                    {
                        "step": f"Investigate scale/unit change in {field} — re-baseline statistics",
                        "priority": "CRITICAL",
                        "deadline": "Immediate",
                    }
                )
            elif change_type == "CHECK_REMOVED":
                migration_checklist.append(
                    {
                        "step": f"Verify intentional removal of check {field}",
                        "priority": "HIGH",
                        "deadline": "Before next deployment",
                    }
                )

        rollback_plan = {
            "steps": [
                "Revert contract to previous snapshot",
                "Re-run validation on baseline data",
                "Verify all checks pass",
                "If using CI/CD, rollback the pipeline version",
            ],
            "estimated_time": "15 minutes",
            "risk_level": "LOW",
        }

        return {
            "exact_diff": diff_result,
            "compatibility_verdict": diff_result["compatibility_verdict"],
            "blast_radius": blast_radius,
            "migration_checklist": migration_checklist,
            "rollback_plan": rollback_plan,
        }

    def run(self):
        """Execute schema evolution analysis."""
        print(f"Analyzing schema evolution for {self.contract_id}...")

        self.load_snapshots()

        if len(self.snapshots) < 2:
            print(
                "Warning: Only one snapshot found. Running generator again to create second snapshot."
            )

        diff_result = self.diff_snapshots()

        print("\nDiff Results:")
        print(f"  Old snapshot: {diff_result.get('old_snapshot', 'N/A')}")
        print(f"  New snapshot: {diff_result.get('new_snapshot', 'N/A')}")
        print(f"  Verdict: {diff_result['compatibility_verdict']}")
        print(f"  Breaking changes: {len(diff_result.get('breaking_changes', []))}")

        migration_report = self.generate_migration_report(diff_result)

        output = {
            "generated_at": datetime.utcnow().isoformat(),
            "contract_id": self.contract_id,
            "schema_diff": diff_result,
            "migration_impact": migration_report,
        }

        Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(self.output_path, "w") as f:
            json.dump(output, f, indent=2)

        print(f"\nOutput written to {self.output_path}")

        return output


def main():
    parser = argparse.ArgumentParser(description="Analyze schema evolution")
    parser.add_argument("--contract-id", required=True, help="Contract ID to analyze")
    parser.add_argument("--output", required=True, help="Path to output JSON report")

    args = parser.parse_args()

    analyzer = SchemaEvolutionAnalyzer(args.contract_id, args.output)
    result = analyzer.run()

    if result["schema_diff"]["compatibility_verdict"] == "BREAKING":
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
