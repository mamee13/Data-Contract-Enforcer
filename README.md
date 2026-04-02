# Data Contract Enforcer

A schema integrity and lineage attribution system for your own platform. This project enforces data contracts across your Week 1-5 systems and detects violations, traces them to their source, and generates compliance reports.

## Prerequisites

- Python 3.11+
- `uv` for dependency management
- Virtual environment activated: `source .venv/bin/activate`

## Installation

```bash
uv sync
```

## Project Structure

```
contracts/
├── generator.py           # ContractGenerator - generates Bitol YAML contracts
├── runner.py             # ValidationRunner - runs contract checks
├── attributor.py         # ViolationAttributor - traces violations to source
├── schema_analyzer.py    # SchemaEvolutionAnalyzer - diffs and classifies changes
├── ai_extensions.py      # AI Contract Extensions - embedding drift, prompt validation, LLM output schema
└── report_generator.py   # Enforcer Report Generator
generated_contracts/      # Auto-generated contract YAML files
validation_reports/       # Validation report JSONs
violation_log/            # Violation records JSONL
schema_snapshots/         # Timestamped schema snapshots
enforcer_report/          # Generated stakeholder reports
outputs/                  # Week 1-5 outputs + traces
```

## Running the Pipeline

### Step 1: Generate Contracts

```bash
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/week3_extractions.yaml
```

**Expected output:** `generated_contracts/week3_extractions.yaml` with min 8 clauses

### Step 2: Run Validation (Clean Data)

```bash
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/clean_run.json
```

**Expected output:** `validation_reports/clean_run.json` - all structural checks PASS

### Step 3: Inject Violation

```bash
python create_violation.py
```

**Expected output:** `outputs/week3/extractions_violated.jsonl` with confidence scaled 0-100

### Step 4: Run Validation (Violated Data)

```bash
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --output validation_reports/violated_run.json
```

**Expected output:** `validation_reports/violated_run.json` - FAIL for confidence range check and FAIL for drift check

### Step 5: Attribute the Violation

```bash
python contracts/attributor.py \
  --violation validation_reports/violated_run.json \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --contract generated_contracts/week3_extractions.yaml \
  --registry contract_registry/subscriptions.yaml \
  --output violation_log/violations.jsonl
```

**Expected output:** `violation_log/violations.jsonl` with blame chain + blast radius

### Step 6: Run Schema Evolution Analysis

```bash
python contracts/schema_analyzer.py \
  --contract-id week3-document-refinery-extractions \
  --output validation_reports/schema_evolution.json
```

**Expected output:** `validation_reports/schema_evolution.json` with diff + compatibility verdict

### Step 7: Run AI Extensions

```bash
python contracts/ai_extensions.py \
  --mode all \
  --extractions outputs/week3/extractions.jsonl \
  --verdicts outputs/week2/verdicts.jsonl \
  --traces outputs/traces/runs.jsonl \
  --output validation_reports/ai_extensions.json
```

**Expected output:** `validation_reports/ai_extensions.json` with embedding drift + prompt validation + LLM output schema results

**Note:** Embedding drift check requires `OPENAI_API_KEY` environment variable. Without it, this check will be skipped.

### Step 8: Generate Enforcer Report

```bash
python contracts/report_generator.py \
  --output enforcer_report/report_data.json
```

**Expected output:** `enforcer_report/report_data.json` with:
- Data Health Score (0-100)
- Violations by severity + top 3 summaries
- Schema changes detected
- AI system risk assessment
- Recommended actions

## Verification Commands

After running all steps, verify the outputs:

```bash
# Check validation reports
ls -la validation_reports/

# Check violations
wc -l violation_log/violations.jsonl

# Check health score
cat enforcer_report/report_data.json | python3 -c "import json,sys; d=json.load(sys.stdin); print('Health Score:', d['data_health_score'])"

# Verify health score is 0-100
python3 -c "import json; d=json.load(open('enforcer_report/report_data.json')); assert 0 <= d['data_health_score'] <= 100, 'Health score out of range'"
```

## Contract Format

Generated contracts follow the Bitol Open Data Contract Standard with:
- Structural clauses (type, required, format)
- Statistical clauses (range, drift detection)
- Lineage context (downstream consumers)
- LLM annotations for ambiguous columns
- dbt schema.yml output

## Key Features

1. **Structural + Statistical Validation**: Catches both schema violations and statistical drift
2. **Blame Chain Attribution**: Traces violations to git commits using lineage traversal
3. **Blast Radius Reporting**: Identifies all downstream consumers affected by violations
4. **Schema Evolution Tracking**: Diffs snapshots and classifies changes as BREAKING/COMPATIBLE
5. **AI-Specific Extensions**: Embedding drift, prompt input validation, LLM output schema enforcement
