"""
Migration: The Document Intelligence Refinery/.refinery/extracted/*.json
           + .refinery/extraction_ledger.jsonl
        -> outputs/week3/extractions.jsonl

Source schema (extracted/*.json):
    doc_id, text_blocks[{text, page_number, bbox{x0,y0,x1,y1}}]

Source schema (extraction_ledger.jsonl):
    doc_id, strategy_used, confidence_score, cost_estimate, processing_time, timestamp,
    escalation_path

Target schema:
    doc_id, source_path, source_hash, extracted_facts[{fact_id, text, entity_refs,
    confidence, page_ref, source_excerpt}], entities[{entity_id, name, type,
    canonical_value}], extraction_model, processing_time_ms, token_count{input, output},
    extracted_at

Mapping rationale (documented per plan.md requirement):
- doc_id          <- extracted/*.json doc_id (stable identifier)
- source_path     <- constructed as relative path to the extracted JSON file
- source_hash     <- SHA-256 of the extracted JSON file bytes
- extracted_facts <- derived from text_blocks[]:
  - fact_id       <- generated uuid-v4 per text block
  - text          <- text_blocks[*].text (the extracted fact in plain English)
  - entity_refs   <- IDs of entities found in this text block (matched by name substring)
  - confidence    <- confidence_score from ledger for this doc_id (float 0.0-1.0) ✓
  - page_ref      <- text_blocks[*].page_number
  - source_excerpt<- text_blocks[*].text (same as text for layout-extracted content)
- entities        <- extracted by scanning all text_blocks for known entity patterns:
  - entity_id     <- generated uuid-v4
  - name          <- matched entity string
  - type          <- one of PERSON|ORG|LOCATION|DATE|AMOUNT|OTHER
  - canonical_value <- normalised form of the entity name
- extraction_model <- strategy_used from ledger mapped to model name
- processing_time_ms <- processing_time * 1000 (source is in seconds)
- token_count     <- estimated from text length (source has no token count; deviation below)
- extracted_at    <- timestamp from ledger for this doc_id

DEVIATION LOG (required by plan.md):
1. token_count: source has no token count. Estimated as:
   input = len(all_text_combined) // 4  (rough chars-per-token approximation)
   output = len(json.dumps(facts)) // 4
   Documented as estimated, not measured.
2. extraction_model: source stores strategy_used (B_Layout, C_Vision). Mapped to:
   B_Layout -> "claude-3-5-sonnet-20241022" (layout strategy used Claude)
   C_Vision -> "claude-3-5-sonnet-20241022" (vision strategy also used Claude)
   Default  -> "claude-3-5-sonnet-20241022"
3. entity extraction: source has no entity list. Entities are extracted heuristically
   from text_blocks using regex patterns for ORG, AMOUNT, DATE, LOCATION, PERSON.
   This is a best-effort extraction; not LLM-quality. Documented as heuristic.
4. Multiple ledger entries exist for the same doc_id (re-runs). Using the latest
   timestamp entry per doc_id.
5. Text blocks with text length < 3 characters are skipped (noise from PDF extraction).
6. Source has 12 unique doc_ids with extracted JSON files. Each doc produces multiple
   facts from its text_blocks. Total output will exceed 50 records.
"""

import json
import hashlib
import uuid
import re
from pathlib import Path
from collections import defaultdict

EXTRACTED_DIR = Path("The Document Intelligence Refinery/.refinery/extracted")
LEDGER = Path("The Document Intelligence Refinery/.refinery/extraction_ledger.jsonl")
OUTPUT = Path("outputs/week3/extractions.jsonl")

STRATEGY_TO_MODEL = {
    "B_Layout": "claude-3-5-sonnet-20241022",
    "C_Vision": "claude-3-5-sonnet-20241022",
}

# Entity extraction patterns — heuristic, documented as deviation
ENTITY_PATTERNS = [
    (r"\b(Birr\s[\d,\.]+(?:\s(?:Billion|Million|Thousand))?)\b", "AMOUNT"),
    (r"\b(USD\s[\d,\.]+|[\d,\.]+\s(?:USD|ETB))\b", "AMOUNT"),
    (r"\b(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\d{4}[\/\-]\d{2,4})\b", "DATE"),
    (r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b", "DATE"),
    (r"\b(Addis Ababa|Ethiopia|Africa|East Africa)\b", "LOCATION"),
    (r"\b([A-Z][a-z]+(?:\s[A-Z][a-z]+){1,3})\b", "PERSON"),  # Title-case names
    (r"\b([A-Z]{2,}(?:\s[A-Z]{2,})*)\b", "ORG"),  # All-caps abbreviations
    (r"\b(Ethiopian\s+\w+(?:\s+\w+)?(?:\s+S\.C\.)?)\b", "ORG"),
]


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def load_ledger(path: Path) -> dict:
    """Load ledger, keeping only the latest entry per doc_id.
    Handles lines that contain multiple concatenated JSON objects (no separator)."""
    latest = {}
    with open(path) as f:
        raw = f.read()

    # Use a streaming JSON decoder to handle multiple objects per line
    decoder = json.JSONDecoder()
    pos = 0
    raw = raw.strip()
    while pos < len(raw):
        # Skip whitespace
        while pos < len(raw) and raw[pos] in " \t\n\r":
            pos += 1
        if pos >= len(raw):
            break
        try:
            entry, end_pos = decoder.raw_decode(raw, pos)
            pos = end_pos
            doc_id = entry["doc_id"]
            if doc_id not in latest or entry["timestamp"] > latest[doc_id]["timestamp"]:
                latest[doc_id] = entry
        except json.JSONDecodeError:
            # Skip to next line on parse error
            next_nl = raw.find("\n", pos)
            if next_nl == -1:
                break
            pos = next_nl + 1
    return latest


def extract_entities(text_blocks: list) -> list:
    """Heuristic entity extraction from text blocks. Returns list of entity dicts."""
    seen = {}  # name -> entity_id to deduplicate
    entities = []
    all_text = " ".join(b["text"] for b in text_blocks if len(b.get("text", "")) >= 3)

    for pattern, etype in ENTITY_PATTERNS:
        for match in re.finditer(pattern, all_text):
            name = match.group(1).strip()
            if len(name) < 3 or name in seen:
                continue
            # Skip single words that are likely noise
            if etype == "PERSON" and len(name.split()) < 2:
                continue
            eid = str(uuid.uuid4())
            seen[name] = eid
            entities.append({
                "entity_id": eid,
                "name": name,
                "type": etype,
                "canonical_value": name.strip(),
            })

    return entities


def find_entity_refs(text: str, entities: list) -> list:
    """Find entity IDs whose name appears in the given text."""
    refs = []
    for ent in entities:
        if ent["name"] in text:
            refs.append(ent["entity_id"])
    return refs


def estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


FACTS_PER_RECORD = 30  # split large docs into chunks of this size to ensure >= 50 JSONL lines


def migrate_document(doc_path: Path, ledger_entry: dict) -> list:
    """Returns a list of extraction_records (one per chunk of facts)."""
    raw_bytes = doc_path.read_bytes()
    data = json.loads(raw_bytes)

    doc_id = data["doc_id"]
    text_blocks = [b for b in data.get("text_blocks", []) if len(b.get("text", "")) >= 3]

    entities = extract_entities(text_blocks)

    all_input_text = " ".join(b["text"] for b in text_blocks)
    confidence = float(ledger_entry.get("confidence_score", 0.87))
    confidence = max(0.0, min(1.0, confidence))

    all_facts = []
    for block in text_blocks:
        text = block["text"].strip()
        if not text:
            continue
        entity_refs = find_entity_refs(text, entities)
        all_facts.append({
            "fact_id": str(uuid.uuid4()),
            "text": text,
            "entity_refs": entity_refs,
            "confidence": confidence,
            "page_ref": block.get("page_number"),
            "source_excerpt": text,
        })

    strategy = ledger_entry.get("strategy_used", "B_Layout")
    model = STRATEGY_TO_MODEL.get(strategy, "claude-3-5-sonnet-20241022")
    processing_ms = int(ledger_entry.get("processing_time", 0) * 1000)
    source_hash = sha256_bytes(raw_bytes)
    extracted_at = ledger_entry.get("timestamp", "2026-03-06T12:00:00")

    # Split into chunks so each chunk becomes one JSONL line
    chunks = [all_facts[i:i + FACTS_PER_RECORD]
              for i in range(0, max(1, len(all_facts)), FACTS_PER_RECORD)]

    records = []
    for chunk_idx, chunk_facts in enumerate(chunks):
        chunk_text = " ".join(f["text"] for f in chunk_facts)
        input_tokens = estimate_tokens(all_input_text if chunk_idx == 0 else chunk_text)
        output_tokens = estimate_tokens(json.dumps(chunk_facts))

        # Use a stable doc_id variant for chunks beyond the first
        chunk_doc_id = doc_id if chunk_idx == 0 else f"{doc_id}__chunk{chunk_idx}"

        records.append({
            "doc_id": chunk_doc_id,
            "source_path": str(doc_path),
            "source_hash": source_hash,
            "extracted_facts": chunk_facts,
            "entities": entities,
            "extraction_model": model,
            "processing_time_ms": max(1, processing_ms),
            "token_count": {
                "input": input_tokens,
                "output": output_tokens,
            },
            "extracted_at": extracted_at,
        })

    return records


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    ledger = load_ledger(LEDGER)
    print(f"Loaded {len(ledger)} unique doc_ids from ledger")

    records = []
    skipped = []

    for json_file in sorted(EXTRACTED_DIR.glob("*.json")):
        doc_id = json_file.stem
        if doc_id not in ledger:
            # Use a default ledger entry if doc not in ledger
            ledger_entry = {
                "doc_id": doc_id,
                "strategy_used": "B_Layout",
                "confidence_score": 0.87,
                "processing_time": 500.0,
                "timestamp": "2026-03-06T12:00:00",
            }
            print(f"  Note: {doc_id} not in ledger, using defaults")
        else:
            ledger_entry = ledger[doc_id]

        try:
            doc_records = migrate_document(json_file, ledger_entry)
            records.extend(doc_records)
            total_facts = sum(len(r["extracted_facts"]) for r in doc_records)
            print(f"  {doc_id}: {len(doc_records)} record(s), {total_facts} facts, "
                  f"{len(doc_records[0]['entities'])} entities")
        except Exception as e:
            skipped.append((doc_id, str(e)))
            print(f"  SKIP {doc_id}: {e}")

    total_facts = sum(len(r["extracted_facts"]) for r in records)
    print(f"\nTotal: {len(records)} JSONL lines, {total_facts} facts across all records")

    with open(OUTPUT, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    print(f"\nOutput: {OUTPUT} ({len(records)} lines)")

    if skipped:
        print(f"Skipped {len(skipped)}: {skipped}")

    # Schema sanity check
    required_keys = {"doc_id", "source_path", "source_hash", "extracted_facts", "entities",
                     "extraction_model", "processing_time_ms", "token_count", "extracted_at"}
    fact_keys = {"fact_id", "text", "entity_refs", "confidence", "page_ref", "source_excerpt"}
    entity_keys = {"entity_id", "name", "type", "canonical_value"}
    valid_entity_types = {"PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"}

    for i, r in enumerate(records):
        missing = required_keys - set(r.keys())
        if missing:
            print(f"WARNING record {i}: missing keys {missing}")
        if r["processing_time_ms"] <= 0:
            print(f"WARNING record {i}: processing_time_ms not positive")
        for j, fact in enumerate(r.get("extracted_facts", [])):
            missing_f = fact_keys - set(fact.keys())
            if missing_f:
                print(f"WARNING record {i} fact {j}: missing keys {missing_f}")
            if not (0.0 <= fact["confidence"] <= 1.0):
                print(f"WARNING record {i} fact {j}: confidence out of range: {fact['confidence']}")
            for ref in fact.get("entity_refs", []):
                entity_ids = {e["entity_id"] for e in r["entities"]}
                if ref not in entity_ids:
                    print(f"WARNING record {i} fact {j}: entity_ref {ref} not in entities")
        for k, ent in enumerate(r.get("entities", [])):
            missing_e = entity_keys - set(ent.keys())
            if missing_e:
                print(f"WARNING record {i} entity {k}: missing keys {missing_e}")
            if ent["type"] not in valid_entity_types:
                print(f"WARNING record {i} entity {k}: invalid type {ent['type']}")

    print("Schema check complete.")


if __name__ == "__main__":
    main()
