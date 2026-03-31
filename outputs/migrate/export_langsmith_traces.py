"""
LangSmith Trace Exporter
========================
Exports all runs from the automaton-auditor-swarm LangSmith project and writes
them to outputs/traces/runs.jsonl in the canonical trace_record schema.

Canonical target schema:
    id, name, run_type, inputs, outputs, error, start_time, end_time,
    total_tokens, prompt_tokens, completion_tokens, total_cost, tags,
    parent_run_id, session_id

Usage:
    uv run outputs/migrate/export_langsmith_traces.py

    # Override project name:
    uv run outputs/migrate/export_langsmith_traces.py --project my-project

    # Export from multiple projects:
    uv run outputs/migrate/export_langsmith_traces.py --project proj1 --project proj2

    # Limit to last N days:
    uv run outputs/migrate/export_langsmith_traces.py --days 90

Required env vars (set in .env or export before running):
    LANGCHAIN_API_KEY   — your LangSmith API key
    LANGCHAIN_PROJECT   — default project name (overridden by --project flag)

The script will:
1. Connect to LangSmith using the API key.
2. List all runs in the specified project(s).
3. Map each run to the canonical trace_record schema.
4. Write to outputs/traces/runs.jsonl (appends if file exists, deduplicates by id).
5. Print a summary of how many records were exported and the final line count.

DEVIATION LOG:
- total_tokens / prompt_tokens / completion_tokens: LangSmith stores these in
  run.prompt_tokens, run.completion_tokens, run.total_tokens. If None, defaults to 0.
- total_cost: stored in run.total_cost. If None, defaults to 0.0.
- session_id: stored in run.session_id. If None, uses run.trace_id as fallback,
  then a generated uuid.
- tags: stored in run.tags. If None, defaults to [].
- start_time / end_time: converted from datetime objects to ISO 8601 strings.
- run_type: validated against {llm, chain, tool, retriever, embedding}.
  Unknown types are mapped to 'chain' and flagged in stderr.
"""

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

OUTPUT = Path("outputs/traces/runs.jsonl")
VALID_RUN_TYPES = {"llm", "chain", "tool", "retriever", "embedding"}
DEFAULT_PROJECT = "automaton-auditor-swarm"
DEFAULT_DAYS = 180


def get_api_key() -> str:
    key = os.environ.get("LANGCHAIN_API_KEY") or os.environ.get("LANGSMITH_API_KEY")
    if not key:
        # Try loading from automaton-auditor-swarm/.env
        env_path = Path("automaton-auditor-swarm/.env")
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                line = line.strip()
                if line.startswith("LANGCHAIN_API_KEY=") or line.startswith("LANGSMITH_API_KEY="):
                    key = line.split("=", 1)[1].strip().strip('"').strip("'")
                    if key and not key.startswith("your_"):
                        break
                    else:
                        key = None
    if not key:
        print("ERROR: LANGCHAIN_API_KEY not found in environment or automaton-auditor-swarm/.env",
              file=sys.stderr)
        print("Set it with: export LANGCHAIN_API_KEY=ls__...", file=sys.stderr)
        sys.exit(1)
    return key


def to_iso(dt) -> str:
    if dt is None:
        return datetime.now(tz=timezone.utc).isoformat()
    if isinstance(dt, str):
        return dt
    if hasattr(dt, "isoformat"):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    return str(dt)


def safe_int(val, default: int = 0) -> int:
    try:
        return int(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def map_run_type(raw_type: str) -> str:
    if raw_type in VALID_RUN_TYPES:
        return raw_type
    # Common aliases
    aliases = {
        "llm_call": "llm",
        "chat": "llm",
        "agent": "chain",
        "workflow": "chain",
        "function": "tool",
        "search": "retriever",
        "embed": "embedding",
    }
    mapped = aliases.get(raw_type, "chain")
    if raw_type not in aliases:
        print(f"  WARNING: unknown run_type '{raw_type}' -> mapped to 'chain'", file=sys.stderr)
    return mapped


def run_to_record(run) -> dict:
    """Map a LangSmith Run object to the canonical trace_record schema."""
    run_id = str(run.id) if run.id else str(uuid.uuid4())

    # Token counts — stored differently across LangSmith SDK versions
    prompt_tokens = safe_int(
        getattr(run, "prompt_tokens", None)
        or getattr(run, "input_tokens", None)
    )
    completion_tokens = safe_int(
        getattr(run, "completion_tokens", None)
        or getattr(run, "output_tokens", None)
    )
    total_tokens = safe_int(
        getattr(run, "total_tokens", None)
    )
    # If total_tokens not set but components are, compute it
    if total_tokens == 0 and (prompt_tokens > 0 or completion_tokens > 0):
        total_tokens = prompt_tokens + completion_tokens

    # session_id fallback chain
    session_id = (
        str(getattr(run, "session_id", None) or "")
        or str(getattr(run, "trace_id", None) or "")
        or str(uuid.uuid4())
    )

    # tags
    tags = list(getattr(run, "tags", None) or [])

    # parent_run_id
    parent_run_id = None
    raw_parent = getattr(run, "parent_run_id", None)
    if raw_parent:
        parent_run_id = str(raw_parent)

    # inputs / outputs — serialize to plain dicts
    inputs = {}
    outputs = {}
    try:
        raw_inputs = getattr(run, "inputs", None)
        if raw_inputs:
            inputs = json.loads(json.dumps(raw_inputs, default=str))
    except Exception:
        inputs = {"_raw": str(getattr(run, "inputs", ""))}

    try:
        raw_outputs = getattr(run, "outputs", None)
        if raw_outputs:
            outputs = json.loads(json.dumps(raw_outputs, default=str))
    except Exception:
        outputs = {"_raw": str(getattr(run, "outputs", ""))}

    return {
        "id": run_id,
        "name": str(getattr(run, "name", "") or ""),
        "run_type": map_run_type(str(getattr(run, "run_type", "chain") or "chain")),
        "inputs": inputs,
        "outputs": outputs,
        "error": getattr(run, "error", None),
        "start_time": to_iso(getattr(run, "start_time", None)),
        "end_time": to_iso(getattr(run, "end_time", None)),
        "total_tokens": total_tokens,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_cost": safe_float(getattr(run, "total_cost", None)),
        "tags": tags,
        "parent_run_id": parent_run_id,
        "session_id": session_id,
    }


def load_existing_ids(path: Path) -> set:
    """Load IDs already in the output file to avoid duplicates."""
    ids = set()
    if not path.exists():
        return ids
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    ids.add(json.loads(line)["id"])
                except Exception:
                    pass
    return ids


def export_project(client, project_name: str, since: datetime,
                   existing_ids: set, output_file) -> int:
    """Export all runs from a project, write new ones to output_file. Returns count written."""
    print(f"\nExporting project: {project_name}")
    print(f"  Since: {since.isoformat()}")

    written = 0
    skipped = 0
    errors = 0

    try:
        runs = client.list_runs(
            project_name=project_name,
            start_time=since,
            execution_order=1,  # only root runs — set to None for all runs including children
        )
    except Exception as e:
        print(f"  ERROR listing runs: {e}", file=sys.stderr)
        # Try without start_time filter
        try:
            runs = client.list_runs(project_name=project_name)
        except Exception as e2:
            print(f"  ERROR (retry): {e2}", file=sys.stderr)
            return 0

    for run in runs:
        try:
            run_id = str(run.id)
            if run_id in existing_ids:
                skipped += 1
                continue
            record = run_to_record(run)
            output_file.write(json.dumps(record) + "\n")
            existing_ids.add(run_id)
            written += 1
            if written % 50 == 0:
                print(f"  ... {written} runs written so far")
        except Exception as e:
            errors += 1
            print(f"  WARNING: failed to process run {getattr(run, 'id', '?')}: {e}",
                  file=sys.stderr)

    print(f"  Done: {written} written, {skipped} skipped (duplicates), {errors} errors")
    return written


def validate_output(path: Path) -> None:
    """Quick schema check on the output file."""
    required = {"id", "name", "run_type", "inputs", "outputs", "error",
                 "start_time", "end_time", "total_tokens", "prompt_tokens",
                 "completion_tokens", "total_cost", "tags", "parent_run_id", "session_id"}
    valid_run_types = {"llm", "chain", "tool", "retriever", "embedding"}

    total = 0
    violations = 0
    with open(path) as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                r = json.loads(line)
                missing = required - set(r.keys())
                if missing:
                    print(f"  WARNING line {i+1}: missing keys {missing}")
                    violations += 1
                if r.get("run_type") not in valid_run_types:
                    print(f"  WARNING line {i+1}: invalid run_type '{r.get('run_type')}'")
                    violations += 1
                if r.get("total_cost", 0) < 0:
                    print(f"  WARNING line {i+1}: total_cost < 0")
                    violations += 1
            except json.JSONDecodeError as e:
                print(f"  WARNING line {i+1}: JSON parse error: {e}")
                violations += 1

    print(f"\nValidation: {total} records, {violations} warnings")
    if total < 50:
        print(f"WARNING: only {total} records — need >= 50. "
              "Run more audits in automaton-auditor-swarm to generate more traces.")
    else:
        print(f"OK: {total} >= 50 minimum requirement met.")


def main():
    parser = argparse.ArgumentParser(description="Export LangSmith traces to JSONL")
    parser.add_argument("--project", action="append", dest="projects",
                        help="Project name(s) to export (can repeat). "
                             f"Default: {DEFAULT_PROJECT}")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS,
                        help=f"Export runs from the last N days. Default: {DEFAULT_DAYS}")
    parser.add_argument("--all-runs", action="store_true",
                        help="Export child runs too (not just root runs). "
                             "Produces more records but larger file.")
    args = parser.parse_args()

    projects = args.projects or [
        os.environ.get("LANGCHAIN_PROJECT", DEFAULT_PROJECT)
    ]

    try:
        from langsmith import Client
    except ImportError:
        print("ERROR: langsmith package not installed. Run: uv add langsmith", file=sys.stderr)
        sys.exit(1)

    api_key = get_api_key()
    client = Client(api_key=api_key)

    since = datetime.now(tz=timezone.utc) - timedelta(days=args.days)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    existing_ids = load_existing_ids(OUTPUT)
    print(f"Existing records in output: {len(existing_ids)}")

    total_written = 0
    with open(OUTPUT, "a") as f:
        for project in projects:
            total_written += export_project(client, project.strip(), since, existing_ids, f)

    print(f"\nTotal new records written: {total_written}")

    # Final line count
    line_count = sum(1 for line in OUTPUT.open() if line.strip())
    print(f"Total records in {OUTPUT}: {line_count}")

    validate_output(OUTPUT)


if __name__ == "__main__":
    main()
