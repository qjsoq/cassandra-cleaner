# Senior Python Developer Review: Risk Assessment

## Overview

This tool extracts partition keys from Cassandra (`ts_kv_cf`), cross-references entities against PostgreSQL, and deletes orphaned partitions. It uses multiprocessing workers, async I/O, and the `dsbulk` CLI. Below are all identified risks, grouped by severity.

---

## CRITICAL — Data Loss / Corruption

### 1. File deleted even after partial Cassandra deletion failures
**Location:** `row_analyzer.py:173-178`

`delete_row_task` catches exceptions and **returns them as values** (`return e` at line 102). `delete_partitions` only logs these failures (line 68-69) but never raises. Control flows back to `get_row_from_file`, which then calls `os.remove(file_path)`. The source file is deleted even though some rows were never successfully deleted from Cassandra. Those rows are lost forever — no retry is possible.

### 2. TOCTOU race condition — entity created between check and delete

`check_rows_in_postgres` queries PostgreSQL, then `delete_partitions` deletes from Cassandra in a separate step. If an entity is created in PostgreSQL *after* the check but *before* the Cassandra delete, its telemetry will be wrongly deleted. There is no locking or versioning to prevent this.

### 3. Reading partially-written CSV files
**Location:** `main.py:43`, `file_seeker.py:30-31`

The threshold is set to **5 seconds** (`threshold_timeout_seconds=5`), but `dsbulk` writes files with `--connector.csv.maxRecords 5000`. If dsbulk takes longer than 5 seconds to finish writing a file, FileSeeker will dispatch it to a worker that will read a truncated/corrupt CSV. `pd.read_csv` on an incomplete file can silently produce partial data or crash.

### 4. `asyncio.gather` without `return_exceptions=True`
**Location:** `row_analyzer.py:156`

```python
results: list[pd.DataFrame | None] = await asyncio.gather(*tasks)
```

If **any** `check_rows_in_postgres` call raises (e.g., the `RuntimeError` on line 108 after 10 retries), the entire `gather` propagates the exception. All other entity-type results are discarded. The file is NOT deleted (which is good), but all work for that file is lost and must be retried from scratch — **if** the file wasn't already removed by the OS error handler.

---

## HIGH — Security

### 5. SQL injection pattern
**Location:** `row_analyzer.py:92`

```python
query: str = f"SELECT id from {table_name} where id = ANY($1::uuid[])"
```

While `table_name` comes from the hardcoded `ENTITY_TABLE_NAME_MAP`, this is an f-string interpolation into a SQL query. If the map is ever modified to include user-derived input, or if a table name contains special characters, this becomes a SQL injection vector. Parameterized table names are not supported by `asyncpg`, but an allowlist validation should be explicitly enforced at the query site.

### 6. Hardcoded default credentials everywhere

| Location | Default |
|---|---|
| `row_analyzer.py:15` | `postgres:postgres` |
| `row_analyzer.py:75-76` | `cassandra:cassandra` |
| `main.py:14` | `127.0.0.1` (no auth) |

If environment variables are unset, the application silently connects with insecure defaults. Production deployments could accidentally use these.

### 7. Database password leakable via DSN
**Location:** `row_analyzer.py:15`

```python
DATABASE_DSN = f"postgresql://...:{urllib.parse.quote_plus(os.getenv('DATABASE_PASSWORD', 'postgres'))}@..."
```

The DSN is a module-level constant. Any exception traceback, debug log, or `repr()` of the connection pool could leak the password. It's also constructed at **import time**, not at connection time.

### 8. No TLS/SSL on any database connection

Neither the Cassandra `Cluster()` nor the `asyncpg.create_pool()` specifies SSL. Data and credentials travel in plaintext.

---

## HIGH — Reliability / Process Management

### 9. FileSeeker never terminates and is NOT a daemon thread
**Location:** `file_seeker.py:46-48`

`start_monitoring()` is an infinite `while True` loop with no exit condition. Unlike `DsBulkReader` (which sets `self.daemon = True`), FileSeeker does not. This means **the Python process will hang on exit** because a non-daemon thread is still running. The `finally` block in `main.py` has no code to stop FileSeeker.

### 10. `dsbulk` retries infinitely on failure
**Location:** `dsbulk_reader.py:45-64`

```python
while True:
    ...
    if self.process.returncode == 0:
        break
    time.sleep(15)
```

If `dsbulk` is misconfigured, missing from `$PATH`, or the Cassandra cluster is down, this loops forever with no max retry count or exponential backoff.

### 11. Dead analyzer detection has no recovery
**Location:** `main.py:54-58`

When a `RowAnalyzer` process dies, the main loop simply breaks and shuts everything down. Files assigned to the dead worker's queue are lost. There is no mechanism to reassign work or restart the worker.

### 12. Bare `except:` clause
**Location:** `file_seeker.py:55-56`

```python
except:
    logger.exception(...)
```

This catches `KeyboardInterrupt`, `SystemExit`, and `GeneratorExit`, making it impossible to cleanly Ctrl+C the application through this thread.

### 13. No signal handling
**Location:** `main.py`

There is no `SIGTERM` / `SIGINT` handler. In a production environment (Docker, systemd), a graceful shutdown signal won't trigger the `finally` block properly in all cases, especially with subprocesses and multiprocessing.

---

## MEDIUM — Bugs

### 14. Round-robin counter resets every cycle
**Location:** `file_seeker.py:23`

```python
def get_file_to_process(self) -> None:
    ...
    self.queue_balancing_counter: int = 0  # BUG: resets the instance variable
```

This re-initializes the counter to 0 **every time** the method is called (every 30 seconds). Files always start filling from queue 0, causing uneven load distribution across workers.

### 15. Unused variable
**Location:** `main.py:34`

```python
was_directory_field: bool = False
```

Declared but never read. Dead code.

### 16. `os.remove` swallows all `OSError`s silently
**Location:** `row_analyzer.py:177-178`

```python
except OSError:
    pass
```

Permission denied, read-only filesystem, file locked — all silently ignored. The worker thinks the file was processed, but it remains on disk and will be rediscovered by FileSeeker, **but** it's already in `processed_files_set`, so it will be skipped. Result: the file is never processed again AND never cleaned up.

### 17. `processed_files_set` grows unboundedly
**Location:** `file_seeker.py:17`

Over a long-running session processing millions of files, this set only grows and is never pruned, causing increasing memory usage.

---

## MEDIUM — Architecture / Design

### 18. No dependency specification

There is no `requirements.txt`, `pyproject.toml`, or `setup.py`. Dependencies include `cassandra-driver`, `asyncpg`, `pandas` — all with version-sensitive APIs. Deploying this to a new environment is guesswork.

### 19. No tests

Zero test files. A tool that deletes production data from Cassandra has no automated verification. The dry-run mode is the only safety net.

### 20. Multiple `logging.basicConfig()` calls
**Location:** every file

Only the first `logging.basicConfig()` call takes effect in a Python process. The calls in `cassandra_row_inferno.py`, `row_analyzer.py`, `dsbulk_reader.py`, and `file_seeker.py` are all silently ignored. Logging configuration should be centralized in `main.py`.

### 21. Hardcoded paths
**Location:** `main.py:13`

```python
WORKING_DIRECTORY = os.getenv("PARTITION_KEYS_DIRECTORY", "/home/thingsboard/tb-test/clean-old/")
```

Default path is specific to a single machine. Combined with `PARTITIONS_DIRECTORY` being derived from the parent directory (`os.path.dirname`), the path logic is fragile.

### 22. `CassandraCleaner` creates `asyncio.Semaphore` in sync `connect()` method
**Location:** `cassandra_row_inferno.py:38`

`asyncio.Semaphore` should ideally be created within the running event loop context. While this works in Python 3.10+ (semaphores are no longer bound to a loop at creation), it's a code smell that couples sync initialization with async runtime state.

### 23. Module-level DSN construction
**Location:** `row_analyzer.py:15`

`DATABASE_DSN` is evaluated at import time. If `RowAnalyzer` is imported in the parent process, the env vars are read in the parent — which is fine for `multiprocessing` fork, but makes the module untestable and inflexible.

---

## LOW — Code Quality

| # | Issue | Location |
|---|---|---|
| 24 | Typo: `entitye_type` | `row_analyzer.py:149` |
| 25 | f-strings with no interpolation: `f"RUNNING IN DRY MODE"` | `main.py:27` |
| 26 | Inconsistent quote styles across the project | Throughout |
| 27 | `raise e` instead of bare `raise` (loses traceback context) | `cassandra_row_inferno.py:42` |
| 28 | `pandas` is heavy for simple CSV reading — `csv.DictReader` would suffice and is already imported but unused | `row_analyzer.py:10` |

---

## Summary — Top 5 Actions to Prioritize

1. **Fix the file-deletion-after-partial-failure bug** (#1) — this is a silent data loss vector in production.
2. **Make FileSeeker a daemon thread or add a stop mechanism** (#9) — the process currently hangs on exit.
3. **Fix the round-robin counter reset bug** (#14) — workers are unevenly loaded.
4. **Increase the file age threshold or coordinate with dsbulk** (#3) — 5 seconds is too aggressive.
5. **Add a `requirements.txt`/`pyproject.toml` and at least integration tests** (#18, #19).
