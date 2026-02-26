# API Reference: `Dataset`

The `Dataset` object is accessed via `db.dataset`. It handles all data ingestion and the calculation of transitive sequences and frequencies.

---

## `ingest()`

```python
db.dataset.ingest(data, col_names: dict, zip_file: str = None, show_progress: bool = True)
```

Ingests EHR observation records from a CSV file into the database. The CSV may be provided as a plain file or inside a ZIP archive. Multiple calls to `ingest()` are supported and will append data to the existing tables.

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `data` | `str` | *(required)* | Filename of the CSV file to ingest. If `zip_file` is provided, this is the name of the CSV *inside* the ZIP. |
| `col_names` | `dict` | *(required)* | Mapping from TSPM field names to column names in your CSV. See below. |
| `zip_file` | `str` | `None` | Full path to a ZIP archive containing the CSV file. |
| `show_progress` | `bool` | `True` | If `True`, prints progress to stdout every 10,000 rows. |

### `col_names` Dictionary

The `col_names` dictionary maps four TSPM-required field names to the actual column names in your CSV file:

| Key | Required | Description |
|---|---|---|
| `"PATIENT"` | ✅ Yes | Column containing the patient identifier string. |
| `"DATE"` | ✅ Yes | Column containing the observation date. |
| `"CODE"` | ✅ Yes | Column containing the observation code (e.g., ICD-9, ICD-10, RxNorm). |
| `"TEXT"` | ❌ No | Column containing a human-readable description of the observation code. |

### Example

```python
col_names = {
    "PATIENT": "patient_id",
    "DATE":    "obs_date",
    "CODE":    "obs_code",
    "TEXT":    "obs_description"
}

# Ingest from a plain CSV file
db.dataset.ingest("observations.csv", col_names)

# Ingest from a CSV inside a ZIP archive
db.dataset.ingest(
    "COVID_35k_subset.csv",
    col_names,
    zip_file="/data/COVID_35k_subset.zip"
)
```

### Notes

- Duplicate rows (same `patient_num`, `obs_code`, `obs_date`) are silently ignored.
- Patient IDs and observation codes are automatically assigned internal integer keys (`patient_num`, `obs_code_id`) and stored in the `lookup_patients` and `lookup_observations` tables.
- You can call `ingest()` multiple times with different CSV files to build up a combined dataset (e.g., ingesting medications and diagnoses separately).

---

## `calculate()`

```python
db.dataset.calculate(
    temporal_buckets: list = [],
    sparsity_threshold: float = 0.05,
    temporal_mode: str = "DAYS"
)
```

Calculates all transitive sequences and population-level frequencies using a parallel multi-process pipeline. This is the core TSPM computation step and should be run after all data has been ingested.

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `temporal_buckets` | `list` | `[]` | Optional list of `(min, max)` tuples defining temporal distance buckets. If empty, raw day counts are used. |
| `sparsity_threshold` | `float` | `0.05` | Minimum fraction of patients (0.0–1.0) that must share a sequence for it to be retained. Default is 5%. |
| `temporal_mode` | `str` | `"DAYS"` | Unit for temporal distance. Currently `"DAYS"` is supported. |

### Temporal Buckets

Temporal buckets group the raw day-count `temporal_distance` into discrete ranges. Each bucket is a `(min_days, max_days)` tuple (inclusive). Sequences whose `temporal_distance` falls outside all defined buckets are assigned a distance of `0`.

```python
bucket_config = [
    (0, 1),    # same day or next day
    (1, 3),    # 1–3 days
    (3, 7),    # 3–7 days
    (7, 30),   # 1 week to 1 month
]
db.dataset.calculate(temporal_buckets=bucket_config, sparsity_threshold=0.01)
```

### Sparsity Threshold

The sparsity threshold controls dimensionality reduction. A sequence `(obs_A → obs_B, temporal_distance)` is retained in the `frequencies` table only if:

```
patient_cnt / total_patients >= sparsity_threshold
```

Lower thresholds retain more sequences (higher dimensionality). Higher thresholds are more aggressive in pruning rare sequences.

### Pipeline Steps

1. **Sequence generation** — Worker processes compute all transitive pairs per patient from `source_data` into temporary per-process SQLite databases.
2. **Frequency aggregation** — Workers aggregate `observation_cnt` (SUM) and `patient_cnt` (COUNT DISTINCT) per `(obs_code_1, obs_code_2, temporal_distance)` into the `frequencies` table.
3. **Sparsity filtering** — Sequences below the threshold are deleted from `frequencies`.
4. **Sequence copy** — Only sequences that survived the sparsity filter are copied into the `sequences` table.
5. **Index creation** — An index on `(obs_code_1, obs_code_2, temporal_distance)` is built on the `sequences` table.

### Example

```python
# Basic calculation with default settings
db.dataset.calculate()

# With temporal bucketing and a tighter sparsity filter
db.dataset.calculate(
    temporal_buckets=[(0, 1), (1, 7), (7, 30), (30, 365)],
    sparsity_threshold=0.01
)
```

### Notes

- `calculate()` always **replaces** any previously calculated sequences and frequencies (it is always destructive on the `sequences` and `frequencies` tables).
- The number of parallel workers is set at `TspmDB` construction time via `parallel_threads`.
- For large datasets, ensure `max_memory_mb` is set generously at construction time to avoid worker memory pressure.

---

## `clear()`

```python
db.dataset.clear()
```

Drops and recreates all database tables, removing all ingested data, sequences, frequencies, and subpopulations. This is a full reset of the database.

### Example

```python
db.dataset.clear()
```

---

## `help()`

```python
db.dataset.help()
```

Prints a summary of available methods to stdout.

