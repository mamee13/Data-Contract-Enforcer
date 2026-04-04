#!/usr/bin/env python3
"""
contracts/attributor.py - ViolationAttributor

Traces contract violations back to their source using lineage traversal
and git blame integration.

Usage:
    python contracts/attributor.py --violation validation_reports/violated_run.json \
                                   --lineage outputs/week4/lineage_snapshots.jsonl \
                                   --contract generated_contracts/week3-document-refinery-extractions.yaml \
                                   --registry contract_registry/subscriptions.yaml \
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
        self,
        violation_path: str,
        lineage_path: str,
        contract_path: str,
        registry_path: str,
        output_path: str,
    ):
        self.violation_path = violation_path
        self.lineage_path = lineage_path
        self.contract_path = contract_path
        self.registry_path = registry_path
        self.output_path = output_path
        self.violation_report: Optional[dict] = None
        self.lineage_snapshot: Optional[dict] = None
        self.contract: Optional[dict] = None
        self.registry: Optional[dict] = None
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

    def load_registry(self):
        """Load the contract registry for blast radius computation."""
        with open(self.registry_path) as f:
            self.registry = yaml.safe_load(f)

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
        edges = self.lineage_snapshot.get("edges", [])

        if not nodes:
            return candidates

        # Build quick lookup maps
        node_by_id = {n.get("node_id"): n for n in nodes if n.get("node_id")}
        reverse_edges: dict[str, list[str]] = {}
        for edge in edges:
            src = edge.get("source")
            tgt = edge.get("target")
            if src and tgt:
                reverse_edges.setdefault(tgt, []).append(src)

        # Seed start nodes by best-effort matching on id/label/path
        start_nodes: list[str] = []
        for node in nodes:
            node_id = node.get("node_id", "")
            label = node.get("label", "")
            path = node.get("metadata", {}).get("path", "")
            haystack = " ".join([node_id, label, path]).lower()
            if failing_system.lower() in haystack:
                start_nodes.append(node_id)

        if not start_nodes:
            return candidates

        # BFS upstream (reverse edges) to locate producer FILE nodes
        visited: set[str] = set(start_nodes)
        frontier: list[tuple[str, int]] = [(n, 0) for n in start_nodes]

        while frontier:
            current, depth = frontier.pop(0)
            node = node_by_id.get(current, {})
            if node.get("type") == "FILE":
                metadata = node.get("metadata", {})
                candidates.append(
                    {
                        "node_id": current,
                        "path": metadata.get("path", current),
                        "type": node.get("type", "FILE"),
                        "lineage_distance": depth,
                    }
                )

            for upstream in reverse_edges.get(current, []):
                if upstream not in visited:
                    visited.add(upstream)
                    frontier.append((upstream, depth + 1))

        # Deduplicate by path and sort by distance
        deduped: dict[str, dict] = {}
        for cand in candidates:
            key = cand.get("path", cand.get("node_id", ""))
            if key not in deduped or cand.get("lineage_distance", 0) < deduped[key].get(
                "lineage_distance", 0
            ):
                deduped[key] = cand

        return sorted(deduped.values(), key=lambda x: x.get("lineage_distance", 0))

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

    def _normalize_field(self, field_name: str) -> str:
        """Normalize column names for registry matching."""
        return field_name.replace("[*]", "").strip()

    def _registry_blast_radius(self, contract_id: str, failing_field: str) -> list[dict]:
        """Return registry subscribers affected by a failing field."""
        assert self.registry is not None
        affected: list[dict] = []
        failing_field = self._normalize_field(failing_field)

        for sub in self.registry.get("subscriptions", []):
            if sub.get("contract_id") != contract_id:
                continue
            for bf in sub.get("breaking_fields", []):
                bf_field = self._normalize_field(bf.get("field", ""))
                if not bf_field:
                    continue
                if (
                    failing_field == bf_field
                    or failing_field.startswith(bf_field)
                    or bf_field.startswith(failing_field)
                ):
                    affected.append(
                        {
                            "subscriber_id": sub.get("subscriber_id"),
                            "subscriber_team": sub.get("subscriber_team"),
                            "validation_mode": sub.get("validation_mode"),
                            "contact": sub.get("contact"),
                            "breaking_field": bf_field,
                            "reason": bf.get("reason"),
                        }
                    )
                    break

        return affected

    def _lineage_enrichment(self, subscriber_ids: list[str], max_depth: int = 2) -> dict:
        """Compute transitive contamination depth from lineage graph."""
        if not self.lineage_snapshot or not subscriber_ids:
            return {"transitive_nodes": [], "contamination_depth": 0}

        nodes = self.lineage_snapshot.get("nodes", [])
        edges = self.lineage_snapshot.get("edges", [])

        start_nodes = set()
        for node in nodes:
            node_id = node.get("node_id", "")
            for sid in subscriber_ids:
                if sid and sid.lower() in node_id.lower():
                    start_nodes.add(node_id)

        if not start_nodes:
            return {"transitive_nodes": [], "contamination_depth": 0}

        visited = set(start_nodes)
        frontier = set(start_nodes)
        transitive: list[str] = []
        depth = 0

        for depth in range(1, max_depth + 1):
            next_frontier = set()
            for edge in edges:
                if edge.get("source") in frontier:
                    target = edge.get("target")
                    if target and target not in visited:
                        visited.add(target)
                        next_frontier.add(target)
                        transitive.append(target)
            if not next_frontier:
                break
            frontier = next_frontier

        return {"transitive_nodes": transitive, "contamination_depth": depth if transitive else 0}

    def compute_blast_radius(self, failing_field: str) -> dict:
        """Compute blast radius from registry, with lineage enrichment."""
        assert self.contract is not None
        assert self.registry is not None

        contract_id = self.violation_report.get("contract_id", "")
        direct_subscribers = self._registry_blast_radius(contract_id, failing_field)
        subscriber_ids = [s.get("subscriber_id") for s in direct_subscribers if s.get("subscriber_id")]
        enrichment = self._lineage_enrichment(subscriber_ids)

        return {
            "direct_subscribers": direct_subscribers,
            "transitive_nodes": enrichment["transitive_nodes"],
            "contamination_depth": enrichment["contamination_depth"],
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
            lineage_distance = file_info.get("lineage_distance", i)

            if file_path and Path(file_path).exists():
                commits = self.get_git_commits(file_path)

                for commit in commits[:3]:
                    confidence = self.compute_confidence_score(
                        commit,
                        lineage_distance=lineage_distance,
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

        blast_radius = self.compute_blast_radius(column_name)
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

        print(f"Loading contract from {self.contract_path}...")
        self.load_contract()

        print(f"Loading registry from {self.registry_path}...")
        self.load_registry()

        print(f"Loading lineage from {self.lineage_path}...")
        self.load_lineage()

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

        comment_line = (
            "# VIOLATION LOG - injected violation documented (see create_violation.py)\n"
        )

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
    parser.add_argument(
        "--registry", required=True, help="Path to contract_registry/subscriptions.yaml"
    )
    parser.add_argument("--output", required=True, help="Path to output violations JSONL")

    args = parser.parse_args()

    attributor = ViolationAttributor(
        args.violation, args.lineage, args.contract, args.registry, args.output
    )

    attributor.run()


if __name__ == "__main__":
    main()
