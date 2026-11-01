# Day 3: Incremental ETL with Checkpoints

Loads a new batch of M-Pesa-style transactions (`incoming/*.json`) into an existing table (`data/transactions.csv`) without reprocessing everything. New records are inserted, newer versions replace older ones, and repeats, stale and invalid records are left out. A checkpoint and a log make every run safe to repeat.

## Approach

`incremental_etl.py` uses only the Python standard library. Each run:

1. **Reads the checkpoint** (`state/checkpoint.json`) to find which batches were already processed. If the file is missing, it starts fresh. If the file is corrupt, it logs a warning and starts fresh.
2. **Loads the existing table** into a dict keyed by `transaction_id`, so each lookup is instant.
3. **Finds every batch** in `incoming/` (sorted by name). A batch already listed in the checkpoint is skipped.
4. **Validates and cleans** each record: strips text and converts `amount_kes` to a number.
5. **Classifies** each record as new, updated, duplicate or invalid (rules below).
6. **Upserts** the new and updated records into a copy of the table.
7. **Saves the table first, then the checkpoint.** Both are written atomically: the script writes to a `.tmp` file and only then replaces the real file. If it crashes halfway through, the old file is still whole.
8. **Logs** every step to the terminal and to `logs/etl.log` (append mode, so every run is kept).

## How to run

Requires Python 3.10 or newer. No packages need to be installed (`requirements.txt` explains why).

```
python -m venv venv
venv\Scripts\activate
python incremental_etl.py
```

On Mac or Linux, activate with `source venv/bin/activate` instead.

Folder layout:

```
data/transactions.csv          the table (updated in place)
incoming/batch_2026-10-21.json new batches go here
state/checkpoint.json          created by the first run
logs/etl.log                   created by the first run
```

To process a new batch, drop the JSON file into `incoming/` and run the script again.

## Classification rules

Records are matched on `transaction_id`. I compare `customer_id`, `merchant`, `amount_kes`, `status`, `channel` and `updated_at`.

| Category | Rule | Action |
| --- | --- | --- |
| **New** | The ID is not in the table | Inserted |
| **Updated** | The ID is in the table, the fields differ, and the incoming `updated_at` is **newer** | Replaces the stored row |
| **Duplicate** | The ID is in the table and every field is identical | Skipped |
| **Duplicate (stale)** | The ID is in the table but the incoming `updated_at` is **older** | Skipped, logged at INFO with both timestamps |
| **Duplicate (in-batch)** | The same ID appears more than once in the batch | The newest copy is kept; the others are counted as duplicates |
| **Invalid** | Missing ID or other required field, amount not a number or not positive, or status not `PENDING` / `COMPLETED` / `FAILED` | Skipped, logged as a WARNING with the reason |

## Assumptions and decisions

- **Amounts with commas are cleaned.** `"3,450.00"` becomes `3450.00`. Only the standard thousands format is accepted (`1,234.56`). An ambiguous value like `"3.450,00"` is rejected as invalid instead of being guessed.
- **Stale records count as duplicates, not invalid.** MP-50012 is not an error: the record is complete and valid, just an older version (COMPLETED at 13:00) than the one already stored (FAILED at 14:45). Applying it would overwrite newer information, so it is skipped. Marking it invalid would raise WARNINGs for something that needs no action (alert fatigue), so it is logged at INFO with both timestamps instead.
- **Zero and negative amounts are invalid.** A payment of -350 KES makes no sense here, so it is rejected rather than stored.
- **In-batch repeats: the newest wins.** If a batch contains the same ID twice, the copy with the latest `updated_at` is used.
- **The table is saved before the checkpoint.** If the script crashes between the two, the next run re-reads the batch, and the duplicate check stops anything being loaded twice. The opposite order could mark a batch "done" when its data was never saved.
- **Two layers of idempotency.** Layer 1: the checkpoint skips batches that were already processed. Layer 2: even without the checkpoint, every record is compared with the table, so nothing is inserted twice (see Run #3).
- **Friendly errors.** If the table is open in Excel (`PermissionError`), a file is missing, or the batch or checkpoint JSON is broken, the script prints one clear message and exits with code 1. The atomic writes mean the table and checkpoint are left safe.

## Data findings

The starting table had **20 rows**. The batch `BATCH-2026-10-21-01` had **17 records**. Before writing any code I went through every record and predicted the result:

| Category | Count | Records |
| --- | --- | --- |
| New | 7 | MP-50021, 50022, 50023 (amount `"3,450.00"`, cleaned), 50024, 50025, 50027, 50028 |
| Updated | 3 | MP-50004 (PENDING → COMPLETED), MP-50008 (PENDING → FAILED), MP-50015 (PENDING → COMPLETED, 960 → 1060) |
| Duplicate | 5 | MP-50002, 50013, 50019 (identical), second copy of MP-50021 (in-batch), MP-50012 (stale: 13:00 vs stored 14:45) |
| Invalid | 2 | Record #10 (`transaction_id` is null), MP-50026 (amount -350) |

The script's first run matched this prediction exactly, ending with **27 rows** (20 + 7 new).

## Evidence

### Run #1: first load

```
2026-11-01T23:30:19 INFO  Run started
2026-11-01T23:30:19 INFO  Existing rows loaded: 20
2026-11-01T23:30:19 INFO  Batch BATCH-2026-10-21-01: 17 records read
2026-11-01T23:30:19 INFO  New: 7 | Updated: 3 | Duplicates: 5 | Invalid: 2
2026-11-01T23:30:19 WARNING Invalid record #10: missing transaction_id
2026-11-01T23:30:19 WARNING Invalid record MP-50026: invalid amount_kes -350.0: must be positive
2026-11-01T23:30:19 INFO  Stale record MP-50012 ignored: stale: incoming updated_at 2026-10-20T13:00:00 is older than stored 2026-10-20T14:45:00
2026-11-01T23:30:19 INFO  Rows after load: 27
2026-11-01T23:30:19 INFO  Run finished in 0.01s
```

### Run #2: same batch again (nothing changes)

```
2026-11-01T23:32:36 INFO  Run started
2026-11-01T23:32:36 INFO  Existing rows loaded: 27
2026-11-01T23:32:36 INFO  Batch BATCH-2026-10-21-01 already processed - skipping
2026-11-01T23:32:36 INFO  New: 0 | Updated: 0 | Duplicates: 0 | Invalid: 0
2026-11-01T23:32:36 INFO  Rows after load: 27
2026-11-01T23:32:36 INFO  Run finished in 0.03s
```
### Run #3: checkpoint deleted, then run again

```
2026-11-01T23:36:30 INFO  Run started
2026-11-01T23:36:30 INFO  Existing rows loaded: 27
2026-11-01T23:36:30 INFO  Batch BATCH-2026-10-21-01: 17 records read
2026-11-01T23:36:30 INFO  New: 0 | Updated: 0 | Duplicates: 15 | Invalid: 2
2026-11-01T23:36:30 WARNING Invalid record #10: missing transaction_id
2026-11-01T23:36:30 WARNING Invalid record MP-50026: invalid amount_kes -350.0: must be positive
2026-11-01T23:36:30 INFO  Stale record MP-50012 ignored: stale: incoming updated_at 2026-10-20T13:00:00 is older than stored 2026-10-20T14:45:00
2026-11-01T23:36:30 INFO  Rows after load: 27
2026-11-01T23:36:30 INFO  Run finished in 0.01s
```
Without the checkpoint, all 15 valid records were recognised as already in the table (7 + 3 + 5). To prove the file itself didn't change, I copied the table before this run and compared it afterwards:

```
fc data\before.csv data\transactions.csv
FC: no differences encountered
```

The full log of every run is in `logs/etl.log`.

## Checkpoint

After the runs, `state/checkpoint.json` holds:

- `processed_batches`: `["BATCH-2026-10-21-01"]`, the batches already loaded, so they are skipped next time
- `high_watermark`: `2026-10-21T11:05:00`, the latest `updated_at` in the table
- `row_count`: `27`, the number of rows after the last run
- `last_run_at`: when the script last ran