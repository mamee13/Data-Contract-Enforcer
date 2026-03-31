#!/usr/bin/env python3
"""
create_violation.py - Inject a known violation for testing

This script injects a confidence scale violation by multiplying confidence
values by 100 (changing from 0.0-1.0 to 0-100 scale).

Injection Method A (required per plan.md):
- Read outputs/week3/extractions.jsonl
- Multiply every extracted_facts[*].confidence value by 100
- Write result to outputs/week3/extractions_violated.jsonl
- Document the injection in violation_log/violations.jsonl comment line
"""

import json
import sys
from pathlib import Path


def inject_confidence_scale_violation(input_path: str, output_path: str) -> int:
    """Multiply all confidence values by 100 to create a scale violation."""
    records = []
    affected_count = 0

    with open(input_path, "r") as f:
        for line in f:
            if not line.strip():
                continue
            record = json.loads(line)

            if "extracted_facts" in record and record["extracted_facts"]:
                for fact in record["extracted_facts"]:
                    if "confidence" in fact:
                        fact["confidence"] = round(fact["confidence"] * 100, 1)
                        affected_count += 1

            records.append(record)

    with open(output_path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    return affected_count


def main():
    input_file = "outputs/week3/extractions.jsonl"
    output_file = "outputs/week3/extractions_violated.jsonl"

    if not Path(input_file).exists():
        print(f"ERROR: Input file {input_file} does not exist", file=sys.stderr)
        sys.exit(1)

    affected = inject_confidence_scale_violation(input_file, output_file)

    print("=" * 60)
    print("INJECTION SUMMARY")
    print("=" * 60)
    print(f"Input file:  {input_file}")
    print(f"Output file: {output_file}")
    print("Change made:  confidence scale changed from 0.0-1.0 to 0-100")
    print(f"Records affected: {affected} confidence values")
    print("=" * 60)
    print("Verification: Run the validation runner against the violated file")
    print("Expected result: FAIL for extracted_facts[*].confidence range check")
    print("=" * 60)


if __name__ == "__main__":
    main()
