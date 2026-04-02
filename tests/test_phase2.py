"""
Full Phase 2 test suite for the Data Contract Enforcer.

Covers every test listed in plan.md:
  - foundation-notes
  - contracts-core
  - validation-core
  - attribution-chain
  - schema-evolution
  - ai-extensions
  - report-readme
  - tooling-packaging
"""

import json
import os
import tempfile
from pathlib import Path

import pytest
import yaml

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _jsonl_records(path: str) -> list:
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                records.append(json.loads(line))
    return records


# ===========================================================================
# FOUNDATION-NOTES
# ===========================================================================

class TestFoundationFolders:
    REQUIRED_DIRS = [
        "contracts",
        "generated_contracts",
        "generated_contracts/prompt_inputs",
        "validation_reports",
        "violation_log",
        "schema_snapshots",
        "enforcer_report",
        "outputs",
        "outputs/quarantine",
        "outputs/migrate",
    ]

    @pytest.mark.parametrize("folder", REQUIRED_DIRS)
    def test_folder_exists(self, folder):
        assert Path(folder).is_dir(), f"Required folder missing: {folder}"


class TestFoundationOutputFiles:
    REQUIRED_FILES = [
        "outputs/week1/intent_records.jsonl",
        "outputs/week2/verdicts.jsonl",
        "outputs/week3/extractions.jsonl",
        "outputs/week4/lineage_snapshots.jsonl",
        "outputs/week5/events.jsonl",
        "outputs/traces/runs.jsonl",
    ]

    @pytest.mark.parametrize("path", REQUIRED_FILES)
    def test_file_exists(self, path):
        assert Path(path).exists(), f"Required output file missing: {path}"


class TestFoundationLineCounts:
    def test_week1_min_10(self):
        assert len(_jsonl_records("outputs/week1/intent_records.jsonl")) >= 10

    def test_week2_min_1(self):
        assert len(_jsonl_records("outputs/week2/verdicts.jsonl")) >= 1

    def test_week3_min_50(self):
        assert len(_jsonl_records("outputs/week3/extractions.jsonl")) >= 50

    def test_week4_min_1(self):
        assert len(_jsonl_records("outputs/week4/lineage_snapshots.jsonl")) >= 1

    def test_week5_min_50(self):
        assert len(_jsonl_records("outputs/week5/events.jsonl")) >= 50

    def test_traces_min_50(self):
        assert len(_jsonl_records("outputs/traces/runs.jsonl")) >= 50


class TestFoundationRecordKeys:
    def test_week1_keys(self):
        rec = _jsonl_records("outputs/week1/intent_records.jsonl")[0]
        required = {"intent_id", "description", "code_refs", "governance_tags", "created_at"}
        assert required.issubset(rec.keys()), f"Missing keys: {required - rec.keys()}"

    def test_week2_keys(self):
        rec = _jsonl_records("outputs/week2/verdicts.jsonl")[0]
        required = {"verdict_id", "target_ref", "rubric_id", "rubric_version",
                    "scores", "overall_verdict", "overall_score", "confidence", "evaluated_at"}
        assert required.issubset(rec.keys()), f"Missing keys: {required - rec.keys()}"

    def test_week3_keys(self):
        rec = _jsonl_records("outputs/week3/extractions.jsonl")[0]
        required = {"doc_id", "source_path", "source_hash", "extracted_facts",
                    "entities", "extraction_model", "processing_time_ms",
                    "token_count", "extracted_at"}
        assert required.issubset(rec.keys()), f"Missing keys: {required - rec.keys()}"

    def test_week4_keys(self):
        rec = _jsonl_records("outputs/week4/lineage_snapshots.jsonl")[0]
        required = {"snapshot_id", "codebase_root", "git_commit", "nodes", "edges", "captured_at"}
        assert required.issubset(rec.keys()), f"Missing keys: {required - rec.keys()}"

    def test_week5_keys(self):
        rec = _jsonl_records("outputs/week5/events.jsonl")[0]
        required = {"event_id", "event_type", "aggregate_id", "aggregate_type",
                    "sequence_number", "payload", "metadata", "schema_version",
                    "occurred_at", "recorded_at"}
        assert required.issubset(rec.keys()), f"Missing keys: {required - rec.keys()}"

    def test_traces_keys(self):
        rec = _jsonl_records("outputs/traces/runs.jsonl")[0]
        required = {"id", "name", "run_type", "inputs", "outputs", "error",
                    "start_time", "end_time", "total_tokens", "prompt_tokens",
                    "completion_tokens", "total_cost", "tags", "parent_run_id", "session_id"}
        assert required.issubset(rec.keys()), f"Missing keys: {required - rec.keys()}"


# ===========================================================================
# CONTRACTS-CORE
# ===========================================================================

CONTRACT_PATHS = [
    "generated_contracts/week1_intent_records.yaml",
    "generated_contracts/week3_extractions.yaml",
    "generated_contracts/week4_lineage.yaml",
    "generated_contracts/week5_events.yaml",
    "generated_contracts/langsmith_traces.yaml",
]

DBT_PATHS = [
    "generated_contracts/week3_extractions_dbt.yml",
    "generated_contracts/week5_events_dbt.yml",
]

PROMPT_INPUT_PATH = "generated_contracts/prompt_inputs/week3_extraction_prompt_input.json"


class TestContractsCore:
    @pytest.mark.parametrize("path", CONTRACT_PATHS)
    def test_yaml_syntax(self, path):
        assert Path(path).exists(), f"Contract missing: {path}"
        with open(path) as f:
            data = yaml.safe_load(f)
        assert data is not None
        assert "id" in data
        assert "checks" in data

    @pytest.mark.parametrize("path", DBT_PATHS)
    def test_dbt_syntax(self, path):
        assert Path(path).exists(), f"dbt schema missing: {path}"
        with open(path) as f:
            data = yaml.safe_load(f)
        assert "models" in data
        assert isinstance(data["models"], list)
        assert len(data["models"]) >= 1

    def test_week3_clause_count(self):
        with open("generated_contracts/week3_extractions.yaml") as f:
            data = yaml.safe_load(f)
        assert len(data.get("checks", [])) >= 8

    def test_week5_clause_count(self):
        with open("generated_contracts/week5_events.yaml") as f:
            data = yaml.safe_load(f)
        assert len(data.get("checks", [])) >= 6

    def test_prompt_input_schema_exists_and_valid(self):
        assert Path(PROMPT_INPUT_PATH).exists()
        with open(PROMPT_INPUT_PATH) as f:
            data = json.load(f)
        assert data.get("$schema") == "http://json-schema.org/draft-07/schema#"
        assert "properties" in data

    @pytest.mark.parametrize("contract_id", [
        "week1-intent-records",
        "week3-document-refinery-extractions",
        "week4-lineage",
        "week5-event-records",
        "langsmith-traces",
    ])
    def test_snapshot_count(self, contract_id):
        snapshot_dir = Path("schema_snapshots") / contract_id
        assert snapshot_dir.exists(), f"Snapshot dir missing: {snapshot_dir}"
        snapshots = list(snapshot_dir.glob("*.yaml"))
        assert len(snapshots) >= 2, f"Expected 2+ snapshots for {contract_id}, got {len(snapshots)}"

    @pytest.mark.parametrize("path", CONTRACT_PATHS)
    def test_lineage_downstream_not_empty(self, path):
        with open(path) as f:
            data = yaml.safe_load(f)
        downstream = data.get("lineage", {}).get("downstream", [])
        assert len(downstream) >= 1, f"lineage.downstream is empty in {path}"


# ===========================================================================
# VIOLATION INJECTION (attribution-chain prerequisite)
# ===========================================================================

class TestViolationInjection:
    def test_violated_file_exists(self):
        assert Path("outputs/week3/extractions_violated.jsonl").exists()

    def test_violated_confidence_above_1(self):
        records = _jsonl_records("outputs/week3/extractions_violated.jsonl")
        for rec in records:
            for fact in rec.get("extracted_facts", []):
                conf = fact.get("confidence")
                if conf is not None:
                    assert conf > 1.0, f"Expected confidence > 1.0, got {conf}"

    def test_original_file_untouched(self):
        """Original extractions.jsonl must still have confidence in 0-1."""
        records = _jsonl_records("outputs/week3/extractions.jsonl")
        for rec in records:
            for fact in rec.get("extracted_facts", []):
                conf = fact.get("confidence")
                if conf is not None:
                    assert 0.0 <= conf <= 1.0, f"Original confidence out of range: {conf}"


# ===========================================================================
# VALIDATION-CORE
# ===========================================================================

REPORT_REQUIRED_FIELDS = {
    "report_id", "contract_id", "snapshot_id", "run_timestamp",
    "total_checks", "passed", "failed", "warned", "errored", "results",
}

RESULT_REQUIRED_FIELDS = {
    "check_id", "column_name", "check_type", "status",
    "actual_value", "expected", "severity", "records_failing",
    "sample_failing", "message",
}


class TestValidationRunner:
    def test_clean_run_report_exists(self):
        assert Path("validation_reports/clean_run.json").exists()

    def test_violated_run_report_exists(self):
        assert Path("validation_reports/violated_run.json").exists()

    def test_clean_run_schema(self):
        with open("validation_reports/clean_run.json") as f:
            report = json.load(f)
        assert REPORT_REQUIRED_FIELDS.issubset(report.keys())

    def test_clean_run_results_schema(self):
        with open("validation_reports/clean_run.json") as f:
            report = json.load(f)
        assert len(report["results"]) > 0
        for result in report["results"]:
            assert RESULT_REQUIRED_FIELDS.issubset(result.keys()), \
                f"Result missing fields: {RESULT_REQUIRED_FIELDS - result.keys()}"

    def test_violated_run_has_fail(self):
        with open("validation_reports/violated_run.json") as f:
            report = json.load(f)
        fail_results = [r for r in report["results"] if r["status"] == "FAIL"]
        assert len(fail_results) >= 1, "Expected at least one FAIL in violated run"

    def test_violated_run_confidence_fail_is_critical(self):
        with open("validation_reports/violated_run.json") as f:
            report = json.load(f)
        confidence_fails = [
            r for r in report["results"]
            if r["status"] == "FAIL" and "confidence" in r.get("check_id", "")
        ]
        assert len(confidence_fails) >= 1
        assert confidence_fails[0]["severity"] == "CRITICAL"

    def test_baselines_created(self):
        assert Path("schema_snapshots/baselines.json").exists()

    def test_baselines_schema(self):
        with open("schema_snapshots/baselines.json") as f:
            data = json.load(f)
        assert "columns" in data
        assert "written_at" in data

    def test_runner_missing_column_produces_error_not_crash(self):
        """Runner must not crash on missing column — produces ERROR result."""
        from contracts.runner import ValidationRunner

        # Write a minimal contract with a non-existent column
        contract = {
            "id": "test-missing-col",
            "checks": [{
                "check_id": "not_null_ghost_col",
                "column_name": "ghost_column",
                "check_type": "not_null",
                "severity": "HIGH",
                "params": {},
            }]
        }
        data_records = [{"doc_id": "x", "source_path": "y"}]

        with tempfile.TemporaryDirectory() as tmpdir:
            contract_path = os.path.join(tmpdir, "contract.yaml")
            data_path = os.path.join(tmpdir, "data.jsonl")
            output_path = os.path.join(tmpdir, "report.json")

            with open(contract_path, "w") as f:
                yaml.dump(contract, f)
            with open(data_path, "w") as f:
                for rec in data_records:
                    f.write(json.dumps(rec) + "\n")

            runner = ValidationRunner(contract_path, data_path, output_path)
            runner.load_contract()
            runner.load_data()
            runner.run_checks()
            report = runner.generate_report()

        # Should have an ERROR result, not a crash
        error_results = [r for r in report["results"] if r["status"] == "ERROR"]
        assert len(error_results) >= 1

    def test_range_check_detects_violated_confidence(self):
        """Range check must FAIL when confidence is 0-100 instead of 0-1."""
        from contracts.runner import ValidationRunner

        contract = {
            "id": "test-range",
            "checks": [{
                "check_id": "range_extracted_facts_confidence",
                "column_name": "extracted_facts[*].confidence",
                "check_type": "range",
                "severity": "CRITICAL",
                "params": {"min": 0.0, "max": 1.0},
            }]
        }
        # Record with confidence = 95 (violated)
        data_records = [{"extracted_facts": [{"confidence": 95.0, "text": "test"}]}]

        with tempfile.TemporaryDirectory() as tmpdir:
            contract_path = os.path.join(tmpdir, "contract.yaml")
            data_path = os.path.join(tmpdir, "data.jsonl")
            output_path = os.path.join(tmpdir, "report.json")

            with open(contract_path, "w") as f:
                yaml.dump(contract, f)
            with open(data_path, "w") as f:
                for rec in data_records:
                    f.write(json.dumps(rec) + "\n")

            runner = ValidationRunner(contract_path, data_path, output_path)
            runner.load_contract()
            runner.load_data()
            runner.run_checks()
            report = runner.generate_report()

        fail_results = [r for r in report["results"] if r["status"] == "FAIL"]
        assert len(fail_results) >= 1


# ===========================================================================
# ATTRIBUTION-CHAIN
# ===========================================================================

class TestAttributionChain:
    def test_violations_file_exists(self):
        assert Path("violation_log/violations.jsonl").exists()

    def test_violations_min_3(self):
        records = _jsonl_records("violation_log/violations.jsonl")
        assert len(records) >= 3, f"Expected >= 3 violations, got {len(records)}"

    def test_violations_has_injected(self):
        """At least one violation must be the injected one (inj-001)."""
        records = _jsonl_records("violation_log/violations.jsonl")
        ids = [r.get("violation_id") for r in records]
        assert "inj-001" in ids, "Injected violation inj-001 not found"

    def test_violations_has_real(self):
        """At least one violation must have a real git commit hash."""
        records = _jsonl_records("violation_log/violations.jsonl")
        real_commits = [
            c
            for r in records
            for c in r.get("blame_chain", [])
            if c.get("commit_hash") not in ("unknown", "", None)
            and len(c.get("commit_hash", "")) >= 7
            and not c.get("commit_hash", "").startswith("a1b2")
        ]
        assert len(real_commits) >= 1, "No real git commit found in any blame chain"

    def test_violation_schema(self):
        records = _jsonl_records("violation_log/violations.jsonl")
        required = {"violation_id", "check_id", "detected_at", "blame_chain", "blast_radius"}
        for rec in records:
            assert required.issubset(rec.keys()), f"Missing fields: {required - rec.keys()}"

    def test_blame_chain_candidate_count(self):
        """Each blame chain must have 1-5 candidates."""
        records = _jsonl_records("violation_log/violations.jsonl")
        for rec in records:
            chain = rec.get("blame_chain", [])
            assert 1 <= len(chain) <= 5, \
                f"Blame chain for {rec['violation_id']} has {len(chain)} candidates (expected 1-5)"

    def test_blast_radius_fields(self):
        records = _jsonl_records("violation_log/violations.jsonl")
        for rec in records:
            br = rec.get("blast_radius", {})
            assert "direct_subscribers" in br
            assert "transitive_nodes" in br
            assert "contamination_depth" in br
            assert "estimated_records" in br

    def test_violations_comment_line(self):
        """violations.jsonl must start with a comment documenting the injected violation."""
        with open("violation_log/violations.jsonl") as f:
            first_line = f.readline()
        assert first_line.startswith("#"), "violations.jsonl must start with a # comment line"
        assert "inj" in first_line.lower() or "inject" in first_line.lower(), \
            "Comment must document the injected violation"


# ===========================================================================
# SCHEMA-EVOLUTION
# ===========================================================================

class TestSchemaEvolution:
    def test_schema_evolution_report_exists(self):
        assert Path("validation_reports/schema_evolution.json").exists()

    def test_snapshot_count_before_diff(self):
        """Both week3 and week5 must have >= 2 snapshots."""
        for contract_id in ["week3-document-refinery-extractions", "week5-event-records"]:
            snapshots = list(Path(f"schema_snapshots/{contract_id}").glob("*.yaml"))
            assert len(snapshots) >= 2, f"Need 2+ snapshots for {contract_id}"

    def test_diff_is_non_empty(self):
        with open("validation_reports/schema_evolution.json") as f:
            data = json.load(f)
        changes = data.get("schema_diff", {}).get("changes", [])
        assert len(changes) >= 1, "Schema diff must be non-empty"

    def test_compatibility_verdict_present(self):
        with open("validation_reports/schema_evolution.json") as f:
            data = json.load(f)
        verdict = data.get("schema_diff", {}).get("compatibility_verdict")
        assert verdict in ("BREAKING", "COMPATIBLE"), f"Unexpected verdict: {verdict}"

    def test_migration_report_completeness(self):
        with open("validation_reports/schema_evolution.json") as f:
            data = json.load(f)
        impact = data.get("migration_impact", {})
        required_sections = {"exact_diff", "compatibility_verdict", "blast_radius",
                             "migration_checklist", "rollback_plan"}
        assert required_sections.issubset(impact.keys()), \
            f"Missing sections: {required_sections - impact.keys()}"

    def test_rollback_plan_has_steps(self):
        with open("validation_reports/schema_evolution.json") as f:
            data = json.load(f)
        rollback = data.get("migration_impact", {}).get("rollback_plan", {})
        assert len(rollback.get("steps", [])) >= 1

    def test_schema_analyzer_classifies_breaking_change(self):
        """Inject a type change and verify it's classified as BREAKING."""
        from contracts.schema_analyzer import SchemaEvolutionAnalyzer

        with tempfile.TemporaryDirectory() as tmpdir:
            contract_id = "test-breaking"
            snap_dir = Path(tmpdir) / "schema_snapshots" / contract_id
            snap_dir.mkdir(parents=True)

            snap1 = {"columns": [{"name": "confidence", "type": "number", "required": False}]}
            snap2 = {"columns": [{"name": "confidence", "type": "string", "required": False}]}

            with open(snap_dir / "snapshot_20260101000000.yaml", "w") as f:
                yaml.dump(snap1, f)
            with open(snap_dir / "snapshot_20260102000000.yaml", "w") as f:
                yaml.dump(snap2, f)

            output_path = os.path.join(tmpdir, "evolution.json")
            analyzer = SchemaEvolutionAnalyzer(contract_id, output_path)
            # Override snapshot dir to use tmpdir
            analyzer.snapshots = []
            for sf in sorted(snap_dir.glob("*.yaml")):
                with open(sf) as f:
                    analyzer.snapshots.append({"path": str(sf), "timestamp": sf.stem, "data": yaml.safe_load(f)})

            diff = analyzer.diff_snapshots()

        assert diff["compatibility_verdict"] == "BREAKING"
        breaking = [c for c in diff["changes"] if c["classification"] == "BREAKING"]
        assert len(breaking) >= 1


# ===========================================================================
# AI-EXTENSIONS
# ===========================================================================

class TestAIExtensions:
    def test_ai_extensions_report_exists(self):
        assert Path("validation_reports/ai_extensions.json").exists()

    def test_embedding_baseline_exists(self):
        assert Path("schema_snapshots/embedding_baselines.npz").exists(), \
            "Embedding baseline not created — run ai_extensions.py first"

    def test_embedding_baseline_not_overwritten_on_second_run(self):
        """Running ai_extensions again must not change the baseline file."""
        baseline_path = Path("schema_snapshots/embedding_baselines.npz")
        assert baseline_path.exists()
        mtime_before = baseline_path.stat().st_mtime

        from contracts.ai_extensions import AIExtensions
        ext = AIExtensions("validation_reports/ai_extensions_test_tmp.json")
        texts = ["sample text one", "sample text two", "another sample"]
        ext.check_embedding_drift(texts)

        mtime_after = baseline_path.stat().st_mtime
        assert mtime_before == mtime_after, "Baseline was overwritten on second run"

        # Cleanup
        Path("validation_reports/ai_extensions_test_tmp.json").unlink(missing_ok=True)

    def test_drift_score_produces_outcome(self):
        with open("validation_reports/ai_extensions.json") as f:
            data = json.load(f)
        drift = data.get("extensions", {}).get("embedding_drift", {})
        assert drift.get("status") in ("PASS", "FAIL", "WARN", "BASELINE_SET", "SKIP")
        assert "drift_score" in drift

    def test_prompt_schema_validation_quarantines_invalid(self):
        """Invalid records (missing doc_id/source_path) must be quarantined."""
        from contracts.ai_extensions import AIExtensions

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "ai_out.json")
            ext = AIExtensions(output_path)

            invalid_records = [
                {"some_field": "no doc_id here"},
                {"doc_id": "valid-id", "source_path": "/some/path"},
            ]

            quarantine_dir = Path("outputs/quarantine")
            quarantine_dir.mkdir(parents=True, exist_ok=True)

            result = ext.validate_prompt_inputs(invalid_records)

        assert result["quarantined_count"] >= 1
        assert result["valid_count"] >= 1
        assert result["status"] == "WARN"

    def test_llm_output_violation_rate_computed(self):
        with open("validation_reports/ai_extensions.json") as f:
            data = json.load(f)
        llm = data.get("extensions", {}).get("llm_output_schema", {})
        assert "violation_rate" in llm
        assert isinstance(llm["violation_rate"], float)

    def test_trace_schema_validation_present(self):
        with open("validation_reports/ai_extensions.json") as f:
            data = json.load(f)
        trace = data.get("extensions", {}).get("trace_schema", {})
        assert "total_traces" in trace
        assert "violation_rate" in trace

    def test_trace_invalid_end_time_produces_violation(self):
        """A trace where end_time < start_time must produce a violation."""
        from contracts.ai_extensions import AIExtensions

        ext = AIExtensions("/tmp/ai_test_tmp.json")
        bad_trace = {
            "id": "trace-bad",
            "start_time": "2026-03-31T10:00:00",
            "end_time": "2026-03-31T09:00:00",  # before start
            "total_tokens": 10,
            "prompt_tokens": 6,
            "completion_tokens": 4,
            "run_type": "llm",
            "total_cost": 0.001,
        }
        result = ext.validate_langsmith_traces([bad_trace])
        assert result["violations"] >= 1


# ===========================================================================
# REPORT-README
# ===========================================================================

REPORT_DATA_REQUIRED_KEYS = {
    "generated_at", "period", "data_health_score", "health_narrative",
    "violations_this_week", "schema_changes", "ai_system_risk", "recommendations",
}


class TestReportReadme:
    def test_report_data_exists(self):
        assert Path("enforcer_report/report_data.json").exists()

    def test_report_data_schema(self):
        with open("enforcer_report/report_data.json") as f:
            data = json.load(f)
        assert REPORT_DATA_REQUIRED_KEYS.issubset(data.keys())

    def test_health_score_range(self):
        with open("enforcer_report/report_data.json") as f:
            data = json.load(f)
        score = data["data_health_score"]
        assert 0 <= score <= 100, f"Health score {score} out of range [0, 100]"

    def test_violations_section_has_by_severity(self):
        with open("enforcer_report/report_data.json") as f:
            data = json.load(f)
        by_sev = data.get("violations_this_week", {}).get("by_severity", {})
        assert set(by_sev.keys()) == {"CRITICAL", "HIGH", "MEDIUM", "LOW"}

    def test_recommendations_count(self):
        with open("enforcer_report/report_data.json") as f:
            data = json.load(f)
        recs = data.get("recommendations", [])
        assert len(recs) >= 1, "Expected at least 1 recommendation"

    def test_ai_risk_has_numeric_values(self):
        with open("enforcer_report/report_data.json") as f:
            data = json.load(f)
        ai = data.get("ai_system_risk", {})
        drift = ai.get("embedding_drift_score")
        rate = ai.get("llm_output_violation_rate")
        # Both should be numeric (not None) after ai_extensions has run
        assert drift is not None, "embedding_drift_score is None"
        assert rate is not None, "llm_output_violation_rate is None"
        assert isinstance(drift, (int, float))
        assert isinstance(rate, (int, float))

    def test_pdf_exists(self):
        pdfs = list(Path("enforcer_report").glob("report_*.pdf"))
        assert len(pdfs) >= 1, "No PDF found in enforcer_report/ — run report_generator.py"

    def test_readme_exists(self):
        assert Path("README.md").exists()

    def test_readme_has_numbered_steps(self):
        content = Path("README.md").read_text()
        # Must have at least 5 numbered steps (### Step N:)
        import re
        steps = re.findall(r"###\s+Step\s+\d+", content)
        assert len(steps) >= 5, f"README has only {len(steps)} numbered steps"


# ===========================================================================
# TOOLING-PACKAGING
# ===========================================================================

class TestToolingPackaging:
    def test_pyproject_toml_exists(self):
        assert Path("pyproject.toml").exists()

    def test_uv_lock_exists(self):
        assert Path("uv.lock").exists()

    def test_pre_commit_config_exists(self):
        assert Path(".pre-commit-config.yaml").exists()

    def test_pre_commit_config_valid_yaml(self):
        with open(".pre-commit-config.yaml") as f:
            data = yaml.safe_load(f)
        assert "repos" in data
        assert len(data["repos"]) >= 1

    def test_dockerfile_exists(self):
        assert Path("Dockerfile").exists()

    def test_dockerfile_has_required_instructions(self):
        content = Path("Dockerfile").read_text()
        assert "FROM python" in content
        assert "COPY" in content
        assert "CMD" in content or "ENTRYPOINT" in content
