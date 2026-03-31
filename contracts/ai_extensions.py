#!/usr/bin/env python3
"""
contracts/ai_extensions.py - AI Contract Extensions

Applies contracts to AI-specific data patterns:
1. Embedding drift detection
2. Prompt input schema validation
3. LLM output schema enforcement

Usage:
    python contracts/ai_extensions.py --mode all \
                                      --extractions outputs/week3/extractions.jsonl \
                                      --verdicts outputs/week2/verdicts.jsonl \
                                      --output validation_reports/ai_extensions.json
"""

import json
import sys
import argparse
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np

try:
    from openai import OpenAI

    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False


class AIExtensions:
    def __init__(self, output_path: str):
        self.output_path = output_path
        self.baseline_path = "schema_snapshots/embedding_baselines.npz"

    def _tfidf_embeddings(self, texts: list) -> np.ndarray:
        """Compute simple TF-IDF-based pseudo-embeddings (no API needed)."""
        from collections import Counter
        import math

        vocab: dict = {}
        tf_vectors = []

        for text in texts:
            tokens = text.lower().split()
            counts = Counter(tokens)
            tf_vectors.append(counts)
            for tok in counts:
                vocab[tok] = vocab.get(tok, 0) + 1

        vocab_list = list(vocab.keys())
        n_docs = len(texts)

        embeddings = []
        for tf in tf_vectors:
            vec = []
            for tok in vocab_list:
                tf_val = tf.get(tok, 0) / max(sum(tf.values()), 1)
                idf_val = math.log((n_docs + 1) / (vocab.get(tok, 0) + 1)) + 1
                vec.append(tf_val * idf_val)
            norm = math.sqrt(sum(v * v for v in vec)) or 1.0
            embeddings.append([v / norm for v in vec])

        return np.array(embeddings)

    def check_embedding_drift(self, text_values: list) -> dict:
        """Check embedding drift. Uses OpenAI if available, else TF-IDF fallback."""
        import os

        sample_size = min(200, len(text_values))
        sample = text_values[:sample_size]

        if not sample:
            return {"status": "SKIP", "drift_score": 0.0, "message": "No text values provided"}

        baseline_exists = Path(self.baseline_path).exists()

        # Try OpenAI first; fall back to TF-IDF
        use_openai = OPENAI_AVAILABLE and bool(os.environ.get("OPENAI_API_KEY"))

        if not baseline_exists:
            print("Creating embedding baseline...")
            if use_openai:
                try:
                    client = OpenAI()
                    response = client.embeddings.create(input=sample, model="text-embedding-3-small")
                    embeddings = np.array([e.embedding for e in response.data])
                except Exception as e:
                    print(f"OpenAI failed ({e}), using TF-IDF fallback")
                    embeddings = self._tfidf_embeddings(sample)
            else:
                embeddings = self._tfidf_embeddings(sample)

            centroid = embeddings.mean(axis=0)
            Path(self.baseline_path).parent.mkdir(parents=True, exist_ok=True)
            np.savez(self.baseline_path, centroid=centroid)

            return {
                "status": "BASELINE_SET",
                "drift_score": 0.0,
                "threshold": 0.15,
                "message": f"Baseline created from {sample_size} texts",
            }

        print("Computing embedding drift...")
        if use_openai:
            try:
                client = OpenAI()
                response = client.embeddings.create(input=sample, model="text-embedding-3-small")
                current_embeddings = np.array([e.embedding for e in response.data])
            except Exception as e:
                print(f"OpenAI failed ({e}), using TF-IDF fallback")
                current_embeddings = self._tfidf_embeddings(sample)
        else:
            current_embeddings = self._tfidf_embeddings(sample)

        current_centroid = current_embeddings.mean(axis=0)

        baseline = np.load(self.baseline_path)
        baseline_centroid = baseline["centroid"]

        # Align dimensions if TF-IDF vocab differs between runs
        min_dim = min(len(current_centroid), len(baseline_centroid))
        current_centroid = current_centroid[:min_dim]
        baseline_centroid = baseline_centroid[:min_dim]

        dot = np.dot(current_centroid, baseline_centroid)
        norm = np.linalg.norm(current_centroid) * np.linalg.norm(baseline_centroid)
        cosine_sim = dot / (norm + 1e-9)

        drift = float(1 - cosine_sim)
        threshold = 0.15

        return {
            "status": "FAIL" if drift > threshold else "PASS",
            "drift_score": round(drift, 4),
            "threshold": threshold,
            "interpretation": "semantic content has shifted" if drift > threshold else "stable",
        }

    def validate_prompt_inputs(self, extraction_records: list) -> dict:
        """Validate prompt input schema and quarantine invalid records."""

        valid_records = []
        quarantined_records = []

        for record in extraction_records:
            has_required = "doc_id" in record and "source_path" in record

            if has_required:
                valid_records.append(record)
            else:
                quarantined_records.append(
                    {"record": record, "error": "Missing required fields: doc_id or source_path"}
                )

        if quarantined_records:
            Path("outputs/quarantine").mkdir(parents=True, exist_ok=True)
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            quarantine_file = f"outputs/quarantine/prompt_inputs_{timestamp}.jsonl"

            with open(quarantine_file, "w") as f:
                for q in quarantined_records:
                    f.write(json.dumps(q) + "\n")

            print(f"Quarantined {len(quarantined_records)} invalid records to {quarantine_file}")

        return {
            "total_records": len(extraction_records),
            "valid_count": len(valid_records),
            "quarantined_count": len(quarantined_records),
            "quarantine_file": quarantine_file if quarantined_records else None,
            "status": "PASS" if len(quarantined_records) == 0 else "WARN",
        }

    def check_llm_output_schema(self, verdict_records: list) -> dict:
        """Check LLM output schema violation rate."""
        total = len(verdict_records)

        if total == 0:
            return {
                "total_outputs": 0,
                "schema_violations": 0,
                "violation_rate": 0.0,
                "trend": "unknown",
                "status": "SKIP",
            }

        violations = 0
        valid_verdicts = {"PASS", "FAIL", "WARN"}

        for record in verdict_records:
            verdict = record.get("overall_verdict")
            if verdict not in valid_verdicts:
                violations += 1

        rate = violations / total
        warn_threshold = 0.02

        baseline_path = "schema_snapshots/llm_output_baseline.json"
        baseline_rate = None

        if Path(baseline_path).exists():
            with open(baseline_path) as f:
                baseline_data = json.load(f)
                baseline_rate = baseline_data.get("baseline_violation_rate")
        else:
            with open(baseline_path, "w") as f:
                json.dump(
                    {"baseline_violation_rate": rate, "created_at": datetime.utcnow().isoformat()},
                    f,
                )

        trend = "unknown"
        if baseline_rate is not None:
            if rate > baseline_rate * 1.5:
                trend = "rising"
            elif rate < baseline_rate * 0.5:
                trend = "falling"
            else:
                trend = "stable"

        return {
            "total_outputs": total,
            "schema_violations": violations,
            "violation_rate": round(rate, 4),
            "baseline_violation_rate": baseline_rate,
            "trend": trend,
            "status": "WARN" if rate > warn_threshold else "PASS",
        }

    def validate_langsmith_traces(self, trace_records: list) -> dict:
        """Validate LangSmith trace schema."""
        violations = []

        for record in trace_records:
            record_violations = []

            start_time = record.get("start_time")
            end_time = record.get("end_time")

            if start_time and end_time:
                try:
                    start = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                    end = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
                    if end < start:
                        record_violations.append("end_time < start_time")
                except Exception:  # noqa: BLE001
                    record_violations.append("invalid datetime format")

            total_tokens = record.get("total_tokens", 0)
            prompt_tokens = record.get("prompt_tokens", 0)
            completion_tokens = record.get("completion_tokens", 0)

            if total_tokens != prompt_tokens + completion_tokens:
                record_violations.append("total_tokens != prompt_tokens + completion_tokens")

            run_type = record.get("run_type")
            valid_run_types = {"llm", "chain", "tool", "retriever", "embedding"}
            if run_type and run_type not in valid_run_types:
                record_violations.append(f"invalid run_type: {run_type}")

            total_cost = record.get("total_cost", 0)
            if total_cost < 0:
                record_violations.append("total_cost < 0")

            if record_violations:
                violations.append({"trace_id": record.get("id"), "violations": record_violations})

        return {
            "total_traces": len(trace_records),
            "violations": len(violations),
            "violation_rate": round(len(violations) / max(len(trace_records), 1), 4),
            "status": "PASS" if len(violations) == 0 else "FAIL",
            "sample_violations": violations[:5],
        }

    def run(
        self,
        mode: str = "all",
        extractions_path: str = None,
        verdicts_path: str = None,
        traces_path: str = None,
    ):
        """Execute AI extensions."""
        results: dict[str, Any] = {"generated_at": datetime.utcnow().isoformat(), "extensions": {}}

        if mode in ("all", "embedding"):
            if extractions_path:
                print("Running embedding drift check...")
                texts = []
                with open(extractions_path) as f:
                    for line in f:
                        if line.strip():
                            record = json.loads(line)
                            for fact in record.get("extracted_facts", []):
                                if "text" in fact:
                                    texts.append(fact["text"])

                results["extensions"]["embedding_drift"] = self.check_embedding_drift(texts)

        if mode in ("all", "prompt"):
            if extractions_path:
                print("Running prompt input validation...")
                records = []
                with open(extractions_path) as f:
                    for line in f:
                        if line.strip():
                            records.append(json.loads(line))

                results["extensions"]["prompt_input"] = self.validate_prompt_inputs(records)

        if mode in ("all", "llm_output"):
            if verdicts_path:
                print("Running LLM output schema check...")
                records = []
                with open(verdicts_path) as f:
                    for line in f:
                        if line.strip():
                            records.append(json.loads(line))

                results["extensions"]["llm_output_schema"] = self.check_llm_output_schema(records)

        if mode in ("all", "traces"):
            if traces_path:
                print("Running LangSmith trace validation...")
                records = []
                with open(traces_path) as f:
                    for line in f:
                        if line.strip():
                            records.append(json.loads(line))

                results["extensions"]["trace_schema"] = self.validate_langsmith_traces(records)

        overall_status = "PASS"
        for ext_name, ext_result in results["extensions"].items():
            status = ext_result.get("status", "SKIP")
            if status in ("FAIL", "ERROR"):
                overall_status = "FAIL"
            elif status == "WARN" and overall_status == "PASS":
                overall_status = "WARN"

        results["overall_status"] = overall_status

        Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(self.output_path, "w") as f:
            json.dump(results, f, indent=2)

        print(f"\n{'=' * 60}")
        print("AI EXTENSIONS SUMMARY")
        print(f"{'=' * 60}")
        print(f"Overall Status: {overall_status}")
        for ext_name, ext_result in results["extensions"].items():
            print(f"  {ext_name}: {ext_result.get('status', 'SKIP')}")
        print(f"{'=' * 60}")
        print(f"Output: {self.output_path}")

        return results


def main():
    parser = argparse.ArgumentParser(description="Run AI contract extensions")
    parser.add_argument(
        "--mode",
        default="all",
        choices=["all", "embedding", "prompt", "llm_output", "traces"],
        help="Extension mode to run",
    )
    parser.add_argument("--extractions", help="Path to extractions JSONL")
    parser.add_argument("--verdicts", help="Path to verdicts JSONL")
    parser.add_argument("--traces", help="Path to traces JSONL")
    parser.add_argument("--output", required=True, help="Path to output JSON")

    args = parser.parse_args()

    extensions = AIExtensions(args.output)
    result = extensions.run(args.mode, args.extractions, args.verdicts, args.traces)

    if result["overall_status"] == "FAIL":
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
