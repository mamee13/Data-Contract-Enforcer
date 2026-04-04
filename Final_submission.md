# TRP Week 7: The Data Contract Enforcer

**Student:** Mamaru Yirga
**Date:** April 4, 2026

---

# Data Contract Enforcer — Submission Report


## 1. Data Health Score

**Score: 0 / 100**

| Metric | Value |
|---|---|
| Total checks run (17 reports) | 361 |
| Checks passed | 339 |
| CRITICAL failures | 19 |
| Base score `(339 / 361) × 100` | 93.91 |
| Penalty `19 × 20` | −380 |
| **Final score** | **0** (floor at 0) |

The base pass rate is 93.91% — the pipeline is structurally sound on most checks. The score collapses to zero because 19 CRITICAL violations each carry a 20-point deduction, which is by design: a single undetected confidence scale change or sequence gap in production is a data integrity failure, not a warning.

---

## 2. Violations This Week

**Period: 2026-03-28 → 2026-04-04**
**Total: 15 violations across 17 validation reports**

| Severity | Count |
|---|---|
| CRITICAL | 14 |
| HIGH | 1 |
| MEDIUM | 0 |
| LOW | 0 |

### Top Failures

**[CRITICAL] `week3-document-refinery-extractions` — `range_extracted_facts_confidence`**
Field: `extracted_facts[*].confidence`
The contract enforces `min: 0.0, max: 1.0` on the confidence field. The violated dataset contains values in the range `[55.0, 95.0]` — a 0–100 integer scale was used instead of the required 0.0–1.0 float scale. **3,791 fact records** are affected. This is a structural violation: the range check and the drift check fire independently, meaning both `range_extracted_facts_confidence` and `mean_drift_extracted_facts_confidence` fail simultaneously (z-score: 14,475.3 — the mean shifted from ~0.75 to ~94.99). The downstream consumer `week4-cartographer` uses confidence scores for node ranking; a 0–100 scale inverts the ordering of every extracted fact in the lineage graph.

**[CRITICAL] `week3-document-refinery-extractions` — `source_hash_unique_per_doc_id`**
Field: `source_hash`
11 SHA-256 hashes appear under more than one `doc_id`, meaning the same source file was re-extracted under different document identifiers. This breaks the deduplication guarantee that the lineage graph relies on for stable node identity.

**[CRITICAL] `week2-verdict-records` — `fk_target_ref_code_refs_file`**
Field: `target_ref`
1 verdict record references a `target_ref` that does not resolve to any `code_refs[*].file` in the Week 1 intent records. The foreign key constraint catches a dangling reference that would silently produce a null join in any downstream query.

**[CRITICAL] `week5-event-records` — `monotonic_sequence_number_per_aggregate`**
Field: `sequence_number`
29 aggregates have non-monotonic sequence numbers (duplicates or gaps). 345 records are affected. Event sourcing correctness depends on gapless monotonic sequences per aggregate — any replay of these aggregates will produce incorrect state.

**[CRITICAL] `week5-event-records` — `payload_schema_per_event_type`**
Field: `payload`
282 event payloads failed JSON Schema validation against the event schema registry at `contract_registry/event_schemas.yaml`. Payloads are validated per `event_type`, so this indicates either unregistered event types or structural drift in payload shape.

**[HIGH] `week2-verdict-records` — `enum_predicted_intent`** *(1 record)*
A verdict record carries a `predicted_intent` value outside the registered enum. Likely a new intent class introduced without a contract update.

---

## 3. Schema Changes Detected

**1 schema evolution report on record.**

The `schema_analyzer` diffed snapshots `snapshot_20260403221335452913` → `snapshot_20260403221708830156` for `week3-document-refinery-extractions` and returned **COMPATIBLE** — the only change between those two snapshots was the addition of the `llm_note` annotation on `processing_time_ms`, which is a metadata-only change.

The more significant evolution event is documented in the violated dataset: the `extracted_facts[*].confidence` field changed from a `float [0.0, 1.0]` scale to an `int [0, 100]` scale. The schema analyzer classifies this as:

```
classification : BREAKING
type           : NARROW_TYPE
severity       : CRITICAL
description    : Type narrowed from number (max=1.0) to integer (max=100)
                 — scale change breaks all downstream range and drift checks
action         : Immediate migration required — re-baseline all statistical checks
```

This is the canonical narrow-type case. Confluent Schema Registry would block this at registration time because it violates backward compatibility. Our analyzer catches it post-hoc by diffing timestamped snapshots — the trade-off is that data can flow before the check runs, which is why the ENFORCE mode exists to block the pipeline on CRITICAL failures.

**Migration checklist for this change:**

1. Revert `extracted_facts[*].confidence` to `float` in the extraction pipeline before the next run.
2. Delete `schema_snapshots/baselines.json` and re-run the generator to re-establish the statistical baseline at the correct 0.0–1.0 scale.
3. Re-run `contracts/runner.py` against `outputs/week3/extractions.jsonl` in ENFORCE mode to confirm zero CRITICAL failures before promoting to the lineage graph.
4. Notify `week4-cartographer` (ENFORCE mode subscriber) that node confidence rankings are invalid for any lineage snapshot produced during the affected window.

**Rollback procedure:**
Revert the contract to the previous snapshot, re-run validation on the baseline data, verify all checks pass, and rollback the pipeline version in CI/CD. Estimated time: 15 minutes. The statistical baselines in `schema_snapshots/baselines.json` must be re-established after rollback — they are invalidated by the scale change.

---

## 4. Violation Deep-Dive — Blame Chain & Blast Radius

### Failing check: `range_extracted_facts_confidence`

**Field:** `extracted_facts[*].confidence`
**Contract:** `generated_contracts/week3-document-refinery-extractions.yaml`
**Actual values:** min=55.0, max=95.0 — **expected:** min≥0.0, max≤1.0
**Records failing:** 3,791

### Lineage traversal

The lineage graph (`outputs/week4/lineage_snapshots.jsonl`) was traversed upstream from the failing schema element `extracted_facts[*].confidence` to identify the producer file. The traversal path:

```
extracted_facts[*].confidence
  └── produced by: outputs/week3/extractions.jsonl  (FILE node, lineage hop 0)
```

### Blame chain

The attributor called `git log --follow --since=14 days ago` on `outputs/week3/extractions.jsonl`.

| Rank | Author | Commit | Score | Message |
|---|---|---|---|---|
| 1 | mamee13 | `e8422abb7073` | **0.9** | Add data outputs for contract validation |

Confidence score formula: `base = 1.0 − (days_since_commit × 0.1) − (lineage_hops × 0.2)`
This commit was 1 day old at detection time → `1.0 − (1 × 0.1) − (0 × 0.2) = 0.9`.

**Attribution confidence: HIGH.** The commit is recent, the file path resolves directly, and there is only one lineage hop between the producer and the failing field. The 0.9 score reflects a same-day change — this is not speculative.

### Blast radius

| Layer | System | Validation mode | Breaking field |
|---|---|---|---|
| Direct subscriber | `week4-cartographer` | **ENFORCE** | `extracted_facts.confidence` |

`week4-cartographer` is registered in ENFORCE mode, meaning it will block its pipeline on this violation. Contamination depth: 0 transitive nodes were reached from the cartographer in the current lineage snapshot, but any lineage graph node that consumed a confidence-ranked extraction during the affected window carries stale rankings.

**Practical consequence:** Every document node in the Week 4 lineage graph that was ranked using confidence scores from the violated dataset has an inverted priority order. A confidence of 0.95 (correct scale) becomes 95.0 (wrong scale), which is 100× larger than any valid value — it will dominate any ranking or aggregation that uses the field numerically.

---

## 5. AI System Risk Assessment

All three AI contract extensions returned **PASS** on the current clean dataset.

### Embedding drift

| Metric | Value |
|---|---|
| Method | Cosine distance between current centroid and stored baseline |
| Baseline path | `schema_snapshots/embedding_baselines.npz` |
| Drift score | **0.0** |
| Threshold | 0.15 |
| Status | **PASS** |

A drift score of 0.0 means the current extraction corpus is semantically identical to the baseline. The baseline was established from 200 sampled `extracted_facts[*].text` values using TF-IDF pseudo-embeddings (OpenAI fallback). The score is persistent across runs — the `.npz` file is loaded on every call and only overwritten when no baseline exists.

### Prompt input schema validation

| Metric | Value |
|---|---|
| Total records | 134 |
| Valid | 134 |
| Quarantined | 0 |
| Quarantine path | `outputs/quarantine/` |
| Status | **PASS** |

All 134 extraction records carry the required `doc_id` and `source_path` fields. Non-conforming records are routed to a timestamped quarantine file rather than dropped silently or passed through — this means failures are auditable.

### LLM output schema violation rate

| Metric | Value |
|---|---|
| Total verdict records | 1 |
| Schema violations | 0 |
| Violation rate | **0.00%** |
| Baseline rate | 0.00% |
| Trend | **stable** |
| Threshold | 2.00% |
| Status | **PASS** |

The violation rate is stable at 0%. If the rate exceeds 2%, the system writes a WARN entry to `violation_log/ai_violations.jsonl` automatically. The baseline was established on first run and persisted to `schema_snapshots/llm_output_baseline.json`.

### LangSmith trace schema

| Metric | Value |
|---|---|
| Total traces | 2,208 |
| Violations | 0 |
| Violation rate | **0.00%** |
| Status | **PASS** |

2,208 LangSmith traces validated against: `end_time > start_time`, `total_tokens = prompt_tokens + completion_tokens`, valid `run_type` enum, and `total_cost ≥ 0`. Zero violations. AI system outputs are currently trustworthy across all four dimensions.

---

## 6. Recommended Actions

Generated from live violation data. File paths and contract clauses are derived from the blame chain and validation reports — not hardcoded.

**Priority 1 — Fix confidence scale in `outputs/week3/extractions.jsonl`**
Contract clause: `range_extracted_facts_confidence`
The extraction pipeline is emitting confidence values on a 0–100 integer scale. Revert to 0.0–1.0 float. After fixing, delete `schema_snapshots/baselines.json` and re-run the generator to re-establish the statistical baseline. This unblocks `week4-cartographer` which is in ENFORCE mode.

**Priority 2 — Resolve source hash collision in `generated_contracts/week3-document-refinery-extractions.yaml`**
Contract clause: `source_hash_unique_per_doc_id`
11 SHA-256 hashes map to more than one `doc_id`. Investigate whether the same source file is being ingested under multiple document identifiers. The fix is upstream in the ingestion pipeline — the contract clause enforces the invariant but cannot repair the data.

**Priority 3 — Fix dangling foreign key in `generated_contracts/week2-verdict-records.yaml`**
Contract clause: `fk_target_ref_code_refs_file`
1 verdict record references a `target_ref` that does not exist in `outputs/week1/intent_records.jsonl:code_refs[*].file`. Identify the orphaned verdict and either correct the reference or remove the record.

---

## 7. Highest-Risk Interface Analysis

**Interface: `week3-document-refinery-extractions` → `week4-cartographer`**
**Schema field: `extracted_facts[*].confidence` (float, 0.0–1.0)**

This is the highest-risk interface in the system. Here is why.

### The failure mode

The confidence field is a float in the range 0.0–1.0. If the extraction pipeline changes the scale to 0–100 (integer or float), every value is still numerically valid from a type perspective — no null, no missing field, no type error. The data flows through silently. This is a **statistical failure**, not a structural one.

### The enforcement gap

The `not_null` check passes. The `format` check passes. Even a naive range check against observed data would pass if the baseline was established on the wrong scale. The only checks that catch this are:

- `range_extracted_facts_confidence` — catches it **only** because the range is semantically hardcoded to `[0.0, 1.0]` in the contract, not derived from observed data. If the contract had been generated from the violated dataset, the range would have been `[55.0, 95.0]` and the check would have passed.
- `mean_drift_extracted_facts_confidence` — catches it because the baseline mean (~0.75) was established on the correct scale. A z-score of 14,475 is unambiguous. This check fires **independently** of the range check, which means both fire simultaneously on a scale change — the two code paths are not coupled.

A check type that would **miss** this: any structural check (not_null, unique, regex, enum). All of them pass on a 0–100 scale. Statistical drift is the only reliable signal.

### The blast radius if this reaches production

`week4-cartographer` is a direct subscriber in ENFORCE mode. Its pipeline blocks on CRITICAL failures, so it would not silently consume the bad data — but it would go down. Any lineage snapshot produced before the ENFORCE block fires carries inverted confidence rankings for every document node. Downstream of the cartographer, `week7-enforcer` (subscriber to `week4-lineage`) would receive a lineage graph with corrupted node weights, and any blame chain confidence scores derived from those weights would be wrong.

### Concrete mitigation

Upgrade the `extracted_facts[*].confidence` drift check from `severity: WARN` to `severity: CRITICAL` in the contract, and add an explicit `check_type: range` with `min: 0.0, max: 1.0` at `severity: CRITICAL` — which is already done. The remaining gap is the generator: if the contract is regenerated from a violated dataset, the semantic range override in `NESTED_PROFILES` must be preserved and must not be overwritten by observed data. The current implementation handles this correctly via `range_override`, but it is a single point of failure — if that override is removed, the contract becomes self-validating against bad data and the check silently passes.

The one concrete addition that would close this gap completely: add a `validation_mode: ENFORCE` upgrade for `week3-document-refinery-extractions` in `contract_registry/subscriptions.yaml` for the `week7-enforcer` subscriber, so that the enforcer itself blocks on a confidence scale change rather than only logging it.
