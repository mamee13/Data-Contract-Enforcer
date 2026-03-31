import os
import json
import yaml
import pytest

# Paths to generated artifacts
CONTRACT_PATHS = [
    "generated_contracts/week1_intent_records.yaml",
    "generated_contracts/week3_extractions.yaml",
    "generated_contracts/week4_lineage.yaml",
    "generated_contracts/week5_events.yaml",
    "generated_contracts/langsmith_traces.yaml"
]

DBT_PATHS = [
    "generated_contracts/week3_extractions_dbt.yml",
    "generated_contracts/week5_events_dbt.yml"
]

PROMPT_INPUT_PATH = "generated_contracts/prompt_inputs/week3_extraction_prompt_input.json"

@pytest.mark.parametrize("path", CONTRACT_PATHS)
def test_yaml_syntax(path):
    """Test that all generated contracts are valid YAML."""
    assert os.path.exists(path), f"Contract file {path} missing."
    with open(path, "r") as f:
        data = yaml.safe_load(f)
        assert data is not None
        assert "id" in data
        assert "checks" in data

@pytest.mark.parametrize("path", DBT_PATHS)
def test_dbt_syntax(path):
    """Test that generated dbt schemas are valid YAML."""
    assert os.path.exists(path), f"dbt schema file {path} missing."
    with open(path, "r") as f:
        data = yaml.safe_load(f)
        assert "models" in data

def test_week3_clause_count():
    """Verify Week 3 has >= 8 clauses."""
    path = "generated_contracts/week3_extractions.yaml"
    with open(path, "r") as f:
        data = yaml.safe_load(f)
        assert len(data.get("checks", [])) >= 8

def test_week5_clause_count():
    """Verify Week 5 has >= 6 clauses."""
    path = "generated_contracts/week5_events.yaml"
    with open(path, "r") as f:
        data = yaml.safe_load(f)
        assert len(data.get("checks", [])) >= 6

def test_prompt_input_schema():
    """Verify the prompt input schema exists and is valid JSON."""
    assert os.path.exists(PROMPT_INPUT_PATH)
    with open(PROMPT_INPUT_PATH, "r") as f:
        data = json.load(f)
        assert data["$schema"] == "http://json-schema.org/draft-07/schema#"

@pytest.mark.parametrize("contract_id", [
    "week1-intent-records",
    "week3-document-refinery-extractions",
    "week4-lineage",
    "week5-event-records",
    "langsmith-traces"
])
def test_snapshot_count(contract_id):
    """Verify each contract has at least 2 timestamped snapshots."""
    snapshot_dir = f"schema_snapshots/{contract_id}"
    assert os.path.exists(snapshot_dir)
    snapshots = [f for f in os.listdir(snapshot_dir) if f.endswith(".yaml")]
    assert len(snapshots) >= 2, f"Expected 2+ snapshots for {contract_id}, found {len(snapshots)}"
