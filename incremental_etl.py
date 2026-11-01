"""
Day 3 - Incremental ETL Pipeline
Squad Python Specialization - Data Engineering

Loads each new JSON batch from incoming/ into data/transactions.csv: new records are
inserted, newer versions replace older ones, and repeats, stale and invalid records
are left out. A checkpoint in state/ and a log in logs/ make every run safe to repeat.

Usage:  python incremental_etl.py
"""

import csv
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

WAREHOUSE_PATH = Path("data") / "transactions.csv"
BATCH_DIR = Path("incoming")
CHECKPOINT_PATH = Path("state") / "checkpoint.json"
LOG_PATH = Path("logs") / "etl.log"

KEY = "transaction_id"
COMPARED_FIELDS = ("customer_id", "merchant", "amount_kes", "status", "channel", "updated_at")
# Amounts written with thousands separators, e.g. "3,450.00", are cleaned (see README).
AMOUNT_PATTERN = re.compile(r"\d+(\.\d{1,2})?|\d{1,3}(,\d{3})+(\.\d{1,2})?")
VALID_STATUSES = {"PENDING", "COMPLETED", "FAILED"}


def setup_logging(path: Path = LOG_PATH) -> logging.Logger:
    """Configure a logger that writes to both the console and a log file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("incremental_etl")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-5s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
    )
    for handler in (logging.StreamHandler(), logging.FileHandler(path, encoding="utf-8")):
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def load_existing(path: Path = WAREHOUSE_PATH) -> dict[str, dict]:
    """Read the loaded transactions into a dict keyed by transaction_id."""
    existing = {}
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row["amount_kes"] = float(row["amount_kes"])
            existing[row[KEY]] = row
    return existing


def load_batch(path: Path) -> tuple[str, list[dict]]:
    """Read one JSON batch file and return (batch_id, transactions)."""
    try:
        with path.open(encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise ValueError(f"batch file not found: {path}")
    except json.JSONDecodeError as error:
        raise ValueError(f"batch file {path.name} is not valid JSON: {error}")

    if not isinstance(data, dict) or not data.get("batch_id") \
            or not isinstance(data.get("transactions"), list):
        raise ValueError(f"batch file {path.name} has no batch_id or transactions list")

    return data["batch_id"], data["transactions"]


def parse_amount(value) -> float | None:
    """Return the amount as a float, or None if it is not a valid number."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and AMOUNT_PATTERN.fullmatch(value.strip()):
        return float(value.strip().replace(",", ""))
    return None


def validate_record(record: dict) -> list[str]:
    """Return the reasons a batch record cannot be loaded (empty list means valid)."""
    reasons = []

    for field in (KEY,) + COMPARED_FIELDS:
        value = record.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            reasons.append(f"missing {field}")

    amount = record.get("amount_kes")
    if "missing amount_kes" not in reasons:
        number = parse_amount(amount)
        if number is None:
            reasons.append(f"invalid amount_kes {amount!r}: not a number")
        elif number <= 0:
            reasons.append(f"invalid amount_kes {amount!r}: must be positive")

    status = record.get("status")
    if status and status not in VALID_STATUSES:
        reasons.append(f"invalid status {status!r}")

    return reasons


def normalise(record: dict) -> dict:
    """Return a clean copy of a valid batch record.

    Table columns only, text stripped, amount as a float."""
    clean = {
        field: record[field].strip() if isinstance(record[field], str) else record[field]
        for field in (KEY,) + COMPARED_FIELDS
    }
    clean["amount_kes"] = parse_amount(record["amount_kes"])
    return clean


def classify(existing: dict[str, dict], batch: list[dict]) -> dict[str, list[dict]]:
    """Split the batch into the buckets 'new', 'updated', 'duplicate' and 'invalid'.

    Use set operations on the keys. A record whose fields are identical to what is
    already loaded is a duplicate; a record that differs is an update only if it is newer.
    """
    buckets = {"new": [], "updated": [], "duplicate": [], "invalid": []}

    # 1. Invalid records go straight to their bucket
    valid = []
    for index, record in enumerate(batch, start=1):
        reasons = validate_record(record)
        if reasons:
            buckets["invalid"].append({**record, "_index": index, "_reasons": reasons})
        else:
            valid.append(normalise(record))

    # 2. Repeats within the batch: keep the newest copy of each ID
    latest = {}
    for record in valid:
        tid = record[KEY]
        if tid not in latest:
            latest[tid] = record
        elif record["updated_at"] > latest[tid]["updated_at"]:
            buckets["duplicate"].append(latest[tid])
            latest[tid] = record
        else:
            buckets["duplicate"].append(record)

    # 3. Set operations on the IDs
    batch_ids = set(latest)
    existing_ids = set(existing)
    new_ids = batch_ids - existing_ids
    overlap_ids = batch_ids & existing_ids

    for tid in sorted(new_ids):
        buckets["new"].append(latest[tid])

    # 4. Compare field values only for IDs that already exist
    for tid in sorted(overlap_ids):
        incoming, stored = latest[tid], existing[tid]
        if all(incoming[field] == stored[field] for field in COMPARED_FIELDS):
            buckets["duplicate"].append(incoming)
        elif incoming["updated_at"] > stored["updated_at"]:
            buckets["updated"].append(incoming)
        else:
            incoming["_note"] = (f"stale: incoming updated_at {incoming['updated_at']} "
                                 f"is older than stored {stored['updated_at']}")
            buckets["duplicate"].append(incoming)

    return buckets


def upsert(existing: dict[str, dict], new: list[dict], updated: list[dict]) -> dict[str, dict]:
    """Return the merged state: inserts for new keys, replacements for updated keys."""
    merged = dict(existing)
    for record in new + updated:
        merged[record[KEY]] = {field: record[field] for field in (KEY,) + COMPARED_FIELDS}
    return merged


def fresh_checkpoint() -> dict:
    """Return an empty checkpoint for a pipeline that has processed nothing yet."""
    return {"last_run_at": None, "processed_batches": [], "high_watermark": None, "row_count": 0}


def read_checkpoint(path: Path = CHECKPOINT_PATH) -> dict:
    """Return the saved checkpoint, or a fresh one if none exists yet."""
    if not path.exists():
        return fresh_checkpoint()
    try:
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as error:
        raise ValueError(f"checkpoint {path} is not valid JSON: {error}")


def write_checkpoint(checkpoint: dict, path: Path = CHECKPOINT_PATH) -> None:
    """Persist the checkpoint so an interrupted run never leaves a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(".tmp")
    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(checkpoint, f, indent=2)
    try:
        temp_path.replace(path)
    except OSError:
        temp_path.unlink(missing_ok=True)
        raise


def save_warehouse(state: dict[str, dict], path: Path = WAREHOUSE_PATH) -> None:
    """Write the merged state back to CSV in a stable order."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(".tmp")
    with temp_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=(KEY,) + COMPARED_FIELDS)
        writer.writeheader()
        for tid in sorted(state):
            row = dict(state[tid])
            row["amount_kes"] = f"{row['amount_kes']:.2f}"
            writer.writerow(row)
    try:
        temp_path.replace(path)
    except OSError:
        temp_path.unlink(missing_ok=True)
        raise


def run() -> dict:
    """Process every batch not yet recorded in the checkpoint and return run statistics."""
    logger = setup_logging()
    started = datetime.now()
    logger.info("Run started")

    try:
        checkpoint = read_checkpoint()
    except ValueError as error:
        logger.warning(f"{error} - starting from a fresh checkpoint (safe: loads are idempotent)")
        checkpoint = fresh_checkpoint()

    state = load_existing()
    logger.info(f"Existing rows loaded: {len(state)}")

    totals = {"new": 0, "updated": 0, "duplicate": 0, "invalid": 0}
    processed_now = []

    for path in sorted(BATCH_DIR.glob("*.json")):
        try:
            batch_id, records = load_batch(path)
        except ValueError as error:
            logger.error(f"Skipping batch file: {error}")
            continue

        if batch_id in checkpoint["processed_batches"]:
            logger.info(f"Batch {batch_id} already processed - skipping")
            continue

        logger.info(f"Batch {batch_id}: {len(records)} records read")
        buckets = classify(state, records)
        logger.info(f"New: {len(buckets['new'])} | Updated: {len(buckets['updated'])} | "
                    f"Duplicates: {len(buckets['duplicate'])} | Invalid: {len(buckets['invalid'])}")
        for item in buckets["invalid"]:
            label = item.get(KEY) or f"#{item['_index']}"
            logger.warning(f"Invalid record {label}: {'; '.join(item['_reasons'])}")
        for item in buckets["duplicate"]:
            if "_note" in item:
                logger.info(f"Stale record {item[KEY]} ignored: {item['_note']}")

        state = upsert(state, buckets["new"], buckets["updated"])
        for name in totals:
            totals[name] += len(buckets[name])
        processed_now.append(batch_id)

    if not processed_now:
        logger.info("New: 0 | Updated: 0 | Duplicates: 0 | Invalid: 0")

    save_warehouse(state)
    checkpoint["processed_batches"] += processed_now
    checkpoint["high_watermark"] = max(
        (row["updated_at"] for row in state.values()), default=checkpoint["high_watermark"]
    )
    checkpoint["row_count"] = len(state)
    checkpoint["last_run_at"] = datetime.now().isoformat(timespec="seconds")
    write_checkpoint(checkpoint)

    logger.info(f"Rows after load: {len(state)}")
    elapsed = (datetime.now() - started).total_seconds()
    logger.info(f"Run finished in {elapsed:.2f}s")
    return {**totals, "rows": len(state), "batches_processed": processed_now}


if __name__ == "__main__":
    try:
        run()
    except PermissionError as error:
        logging.getLogger("incremental_etl").error(
            f"Cannot write {error.filename2 or error.filename}: is it open in Excel? "
            "Close it and run again. The table and checkpoint were left safe."
        )
        sys.exit(1)  
    except FileNotFoundError as error:
        logging.getLogger("incremental_etl").error(f"Required file not found: {error.filename}")
        sys.exit(1)
    except ValueError as error:
        logging.getLogger("incremental_etl").error(f"Run stopped: {error}")
        sys.exit(1)
