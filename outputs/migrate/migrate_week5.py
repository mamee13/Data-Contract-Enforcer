"""
Migration: agentic-event-ledger/data/seed_events.jsonl
        -> outputs/week5/events.jsonl

Source schema:
    stream_id, event_type, event_version, payload{...}, recorded_at

Target schema:
    event_id, event_type, aggregate_id, aggregate_type, sequence_number, payload,
    metadata{causation_id, correlation_id, user_id, source_service}, schema_version,
    occurred_at, recorded_at

Mapping rationale (documented per plan.md requirement):
- event_id        <- generated uuid-v4 per event (source has no event_id)
- event_type      <- event_type (already PascalCase ✓)
- aggregate_id    <- derived from stream_id:
                     "loan-APEX-0001" -> aggregate_id = "APEX-0001"
                     "docpkg-APEX-0001" -> aggregate_id = "APEX-0001"
- aggregate_type  <- derived from stream_id prefix:
                     "loan-*" -> "LoanApplication"
                     "docpkg-*" -> "DocumentPackage"
                     default -> "Unknown"
- sequence_number <- assigned per aggregate_id in order of recorded_at (1-indexed,
                     monotonically increasing, no gaps) ✓
- payload         <- payload (passed through as-is)
- metadata        <- constructed:
  - causation_id  <- null (source has no causation chain)
  - correlation_id <- aggregate_id (events on same aggregate share correlation)
  - user_id       <- payload.uploaded_by if present, else "system"
  - source_service <- "week5-agentic-event-ledger"
- schema_version  <- event_version as string (e.g. 1 -> "1.0")
- occurred_at     <- payload.*_at if present (submitted_at, created_at, uploaded_at,
                     added_at), else recorded_at
- recorded_at     <- recorded_at (passed through) ✓

DEVIATION LOG (required by plan.md):
1. event_id: source has no event_id. Generated fresh uuid-v4 per record.
2. causation_id: source has no causation chain. Set to null.
3. occurred_at: source has no explicit occurred_at field. Derived from payload
   timestamp fields in priority order: submitted_at > created_at > uploaded_at >
   added_at > recorded_at. All derived values satisfy occurred_at <= recorded_at.
4. sequence_number: source has no sequence_number. Assigned by sorting events per
   aggregate_id by recorded_at ascending and numbering from 1. This guarantees
   monotonically increasing, no gaps, no duplicates per aggregate_id.
5. Source has 1218 lines — well above the 50-record minimum. All records migrated.
"""

import json
import uuid
from pathlib import Path
from collections import defaultdict

SOURCE = Path("agentic-event-ledger/data/seed_events.jsonl")
OUTPUT = Path("outputs/week5/events.jsonl")

STREAM_PREFIX_TO_AGGREGATE_TYPE = {
    "loan": "LoanApplication",
    "docpkg": "DocumentPackage",
    "review": "ReviewProcess",
    "decision": "CreditDecision",
    "disbursement": "Disbursement",
    "repayment": "RepaymentSchedule",
}

OCCURRED_AT_PAYLOAD_KEYS = [
    "submitted_at", "created_at", "uploaded_at", "added_at",
    "requested_at", "decided_at", "disbursed_at", "scheduled_at",
]


def parse_stream_id(stream_id: str) -> tuple:
    """Returns (aggregate_type, aggregate_id)."""
    parts = stream_id.split("-", 1)
    if len(parts) == 2:
        prefix = parts[0].lower()
        agg_id = parts[1]
        agg_type = STREAM_PREFIX_TO_AGGREGATE_TYPE.get(prefix, "Unknown")
        return agg_type, agg_id
    return "Unknown", stream_id


def get_occurred_at(payload: dict, recorded_at: str) -> str:
    for key in OCCURRED_AT_PAYLOAD_KEYS:
        if key in payload and payload[key]:
            return payload[key]
    return recorded_at


def get_user_id(payload: dict) -> str:
    for key in ("uploaded_by", "requested_by", "decided_by", "user_id", "created_by"):
        if key in payload and payload[key]:
            return str(payload[key])
    return "system"


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    raw_records = []
    with open(SOURCE) as f:
        for line in f:
            line = line.strip()
            if line:
                raw_records.append(json.loads(line))

    print(f"Loaded {len(raw_records)} source records")

    # Group by (aggregate_id, aggregate_type) — scoped per stream prefix to avoid
    # sequence collisions between loan-APEX-0001 and docpkg-APEX-0001
    aggregate_groups = defaultdict(list)
    for line_idx, raw in enumerate(raw_records):
        agg_type, agg_id = parse_stream_id(raw["stream_id"])
        # Include line_idx as tiebreaker so identical recorded_at values get distinct seq numbers
        aggregate_groups[(agg_id, agg_type)].append((raw["recorded_at"], line_idx, agg_type, raw))

    # Sort each group by (recorded_at, line_idx) ascending — stable, no ties
    for key in aggregate_groups:
        aggregate_groups[key].sort(key=lambda x: (x[0], x[1]))

    # Build lookup: (stream_id, line_idx) -> (seq_num, agg_type, agg_id)
    seq_lookup = {}
    for (agg_id, agg_type), group in aggregate_groups.items():
        for seq_num, (recorded_at, line_idx, _, raw) in enumerate(group, start=1):
            seq_lookup[(raw["stream_id"], line_idx)] = (seq_num, agg_type, agg_id)

    migrated = []
    for line_idx, raw in enumerate(raw_records):
        agg_type, agg_id = parse_stream_id(raw["stream_id"])
        seq_num, agg_type, agg_id = seq_lookup.get(
            (raw["stream_id"], line_idx), (1, agg_type, agg_id)
        )

        payload = raw.get("payload", {})
        occurred_at = get_occurred_at(payload, raw["recorded_at"])
        user_id = get_user_id(payload)

        # schema_version: event_version int -> "N.0" string
        schema_version = f"{raw.get('event_version', 1)}.0"

        migrated.append({
            "event_id": str(uuid.uuid4()),
            "event_type": raw["event_type"],
            "aggregate_id": agg_id,
            "aggregate_type": agg_type,
            "sequence_number": seq_num,
            "payload": payload,
            "metadata": {
                "causation_id": None,
                "correlation_id": agg_id,
                "user_id": user_id,
                "source_service": "week5-agentic-event-ledger",
            },
            "schema_version": schema_version,
            "occurred_at": occurred_at,
            "recorded_at": raw["recorded_at"],
        })

    with open(OUTPUT, "w") as f:
        for r in migrated:
            f.write(json.dumps(r) + "\n")

    print(f"Migrated {len(migrated)} event records")
    print(f"Output: {OUTPUT}")

    # Schema sanity check
    required_keys = {"event_id", "event_type", "aggregate_id", "aggregate_type",
                     "sequence_number", "payload", "metadata", "schema_version",
                     "occurred_at", "recorded_at"}
    meta_keys = {"causation_id", "correlation_id", "user_id", "source_service"}

    # Check sequence_number monotonicity per (aggregate_id, aggregate_type)
    seq_by_agg = defaultdict(list)
    for r in migrated:
        seq_by_agg[(r["aggregate_id"], r["aggregate_type"])].append(r["sequence_number"])

    for agg_key, seqs in seq_by_agg.items():
        sorted_seqs = sorted(seqs)
        expected = list(range(1, len(seqs) + 1))
        if sorted_seqs != expected:
            print(f"WARNING: aggregate {agg_key} sequence numbers not monotonic: {sorted_seqs[:10]}")

    violations = 0
    for i, r in enumerate(migrated):
        missing = required_keys - set(r.keys())
        if missing:
            print(f"WARNING record {i}: missing keys {missing}")
            violations += 1
        missing_meta = meta_keys - set(r.get("metadata", {}).keys())
        if missing_meta:
            print(f"WARNING record {i}: missing metadata keys {missing_meta}")
            violations += 1
        if r["occurred_at"] > r["recorded_at"]:
            print(f"WARNING record {i}: occurred_at > recorded_at")
            violations += 1

    if violations == 0:
        print("Schema check complete — no violations.")
    else:
        print(f"Schema check complete — {violations} warnings.")


if __name__ == "__main__":
    main()
