"""
Migration: automaton-auditor-swarm/audit/report_onself_generated/report_automaton-auditor-swarm.json
        -> outputs/week2/verdicts.jsonl

Source schema:  verdict, dimension_scores{criterion: score}, dissent_summary,
                remediation_plan, raw_opinions[{judge, criterion_id, score, argument,
                cited_evidence_ids}]
Target schema:  verdict_id, target_ref, rubric_id, rubric_version, scores{criterion:
                {score, evidence, notes}}, overall_verdict, overall_score, confidence,
                evaluated_at

Rubric file:    automaton-auditor-swarm/rubric/week2_rubric.json
                rubric_id = SHA-256 of the rubric file content (hex digest)

Mapping rationale (documented per plan.md requirement):
- verdict_id      <- generated uuid-v4 (source has no ID field)
- target_ref      <- "automaton-auditor-swarm" (the audited repo, inferred from filename)
- rubric_id       <- sha256(rubric file bytes) — enables hash check in ValidationRunner
- rubric_version  <- rubric_metadata.version from rubric file
- scores          <- dimension_scores keys, with evidence from raw_opinions per criterion
  - score         <- dimension_scores[criterion] (int, already 0-35 range; normalised to 1-5)
  - evidence      <- cited_evidence_ids from all judges for that criterion
  - notes         <- concatenated judge arguments for that criterion
- overall_verdict <- mapped from source verdict string:
                     "Partial Success" -> WARN, "Success" -> PASS, else -> FAIL
- overall_score   <- weighted mean of normalised scores (equal weights per criterion)
- confidence      <- 0.82 (mid-high; source has no confidence; documented deviation)
- evaluated_at    <- file mtime as ISO 8601 (source has no timestamp; documented deviation)

DEVIATION LOG (required by plan.md):
1. verdict_id: source has no ID. Generated fresh uuid-v4.
2. scores[*].score: source stores raw dimension scores (0-35 scale per criterion).
   Normalised to 1-5 scale: score_1_5 = round((raw / max_score) * 4) + 1, clamped to [1,5].
   max_score per criterion read from rubric file.
3. confidence: source has no confidence field. Set to 0.82 (reasonable for a multi-judge
   system with dissent present). In 0.0-1.0 range.
4. evaluated_at: source has no timestamp. Using file modification time.
5. overall_score: computed as weighted mean of normalised 1-5 scores (equal weights).
   This satisfies the ValidationRunner check: overall_score == weighted mean of scores dict.
6. Only one verdict record exists in source. This is >= 1 minimum so no padding needed.
   Additional verdict records from peer reports are added if available.
"""

import json
import hashlib
import uuid
from pathlib import Path
from datetime import datetime, timezone

SOURCE = Path("automaton-auditor-swarm/audit/report_onself_generated/report_automaton-auditor-swarm.json")
RUBRIC = Path("automaton-auditor-swarm/rubric/week2_rubric.json")
PEER_REPORT_DIR = Path("automaton-auditor-swarm/audit/report_bypeer_received")
OUTPUT = Path("outputs/week2/verdicts.jsonl")


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def get_rubric_max_scores(rubric: dict) -> dict:
    return {d["id"]: d["max_score"] for d in rubric.get("dimensions", [])}


def normalise_score(raw: int, max_score: int) -> int:
    """Normalise raw score (0-max_score) to 1-5 scale."""
    if max_score <= 0:
        return 3
    ratio = raw / max_score
    return max(1, min(5, round(ratio * 4) + 1))


def map_verdict(verdict_str: str) -> str:
    v = verdict_str.lower()
    if "partial" in v or "warn" in v or "remediation" in v:
        return "WARN"
    if "success" in v or "pass" in v:
        return "PASS"
    return "FAIL"


def build_scores(dimension_scores: dict, raw_opinions: list, max_scores: dict) -> dict:
    scores = {}
    for criterion, raw_score in dimension_scores.items():
        max_s = max_scores.get(criterion, 25)
        norm = normalise_score(raw_score, max_s)

        evidence = []
        notes_parts = []
        for op in raw_opinions:
            if op.get("criterion_id") == criterion:
                evidence.extend(op.get("cited_evidence_ids", []))
                notes_parts.append(f"[{op['judge']}] {op.get('argument', '')[:200]}")

        scores[criterion] = {
            "score": norm,
            "evidence": list(dict.fromkeys(evidence)),  # deduplicate, preserve order
            "notes": " | ".join(notes_parts)[:500],
        }
    return scores


def compute_overall_score(scores: dict) -> float:
    if not scores:
        return 3.0
    values = [v["score"] for v in scores.values()]
    return round(sum(values) / len(values), 2)


def get_evaluated_at(path: Path) -> str:
    mtime = path.stat().st_mtime
    return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()


def migrate_report(source_path: Path, rubric_id: str, rubric_version: str,
                   max_scores: dict, target_ref: str) -> dict:
    with open(source_path) as f:
        data = json.load(f)

    scores = build_scores(
        data.get("dimension_scores", {}),
        data.get("raw_opinions", []),
        max_scores,
    )
    overall_score = compute_overall_score(scores)
    overall_verdict = map_verdict(data.get("verdict", ""))

    return {
        "verdict_id": str(uuid.uuid4()),
        "target_ref": target_ref,
        "rubric_id": rubric_id,
        "rubric_version": rubric_version,
        "scores": scores,
        "overall_verdict": overall_verdict,
        "overall_score": overall_score,
        "confidence": 0.82,  # DEVIATION: no source confidence, defaulting to 0.82
        "evaluated_at": get_evaluated_at(source_path),
    }


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    rubric_data = json.loads(RUBRIC.read_text())
    rubric_id = sha256_file(RUBRIC)
    rubric_version = rubric_data["rubric_metadata"]["version"]
    max_scores = get_rubric_max_scores(rubric_data)

    records = []

    # Primary: self-generated report
    record = migrate_report(SOURCE, rubric_id, rubric_version, max_scores,
                            target_ref="automaton-auditor-swarm")
    records.append(record)

    # Secondary: any peer-received reports (JSON files in peer dir)
    if PEER_REPORT_DIR.exists():
        for peer_file in PEER_REPORT_DIR.glob("*.json"):
            try:
                peer_record = migrate_report(peer_file, rubric_id, rubric_version,
                                             max_scores, target_ref=peer_file.stem)
                records.append(peer_record)
            except Exception as e:
                print(f"Skipping {peer_file.name}: {e}")

    with open(OUTPUT, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    print(f"Migrated {len(records)} verdict record(s)")
    print(f"rubric_id (SHA-256): {rubric_id}")
    print(f"Output: {OUTPUT}")

    # Schema sanity check
    required_keys = {"verdict_id", "target_ref", "rubric_id", "rubric_version",
                     "scores", "overall_verdict", "overall_score", "confidence", "evaluated_at"}
    for i, r in enumerate(records):
        missing = required_keys - set(r.keys())
        if missing:
            print(f"WARNING: record {i} missing keys: {missing}")
        if r["overall_verdict"] not in {"PASS", "FAIL", "WARN"}:
            print(f"WARNING: record {i} invalid overall_verdict: {r['overall_verdict']}")
        if not (0.0 <= r["confidence"] <= 1.0):
            print(f"WARNING: record {i} confidence out of range: {r['confidence']}")
        for crit, s in r["scores"].items():
            if not (1 <= s["score"] <= 5):
                print(f"WARNING: record {i} criterion {crit} score out of 1-5: {s['score']}")
    print("Schema check complete.")


if __name__ == "__main__":
    main()
