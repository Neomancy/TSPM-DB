# API Reference: `TspmDB`

The `TspmDB` class is the top-level entry point for the library. It opens (or creates) a SQLite database file and exposes the three primary sub-objects: `dataset`, `population`, and `subpopulation`.

---

## Constructor

```python
TspmDB(db_path: str, destructive: bool = False, parallel_threads: int = 1, max_memory_mb: int = 2048)
```

Opens an existing TSPM database or creates a new one at the given path.

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `db_path` | `str` | *(required)* | Path to the SQLite database file. Created if it does not exist. |
| `destructive` | `bool` | `False` | If `True`, clears existing data when re-initializing tables. Use with caution. |
| `parallel_threads` | `int` | `1` | Number of parallel worker processes to use during sequence calculation. |
| `max_memory_mb` | `int` | `2048` | Maximum memory (in MB) allocated across all worker processes during calculation. |

### Example

```python
import tspmdb

# Open or create a database
db = tspmdb.TspmDB("my_study.sqlite3", parallel_threads=4, max_memory_mb=4096)
```

---

## Properties

### `db.conn`

The underlying `sqlite3.Connection` object. Available for advanced users who need to run custom SQL queries directly.

```python
cur = db.conn.cursor()
cur.execute("SELECT COUNT(*) FROM lookup_patients")
print(cur.fetchone()[0])
```

### `db.dataset`

Returns the [`Dataset`](api_dataset.md) object for ingesting data and calculating sequences.

```python
db.dataset.ingest("observations.csv", col_names)
db.dataset.calculate()
```

### `db.population`

Returns the [`Population`](api_population.md) object for querying all patients, sequences, and frequencies in the database.

```python
patients = db.population.patients()
freqs = db.population.frequencies(as_pandas=True)
```

### `db.subpopulation`

Returns the [`Subpopulation`](api_subpopulation.md) object for creating and managing patient subpopulations.

```python
cohort = db.subpopulation.create("diabetic_patients", patient_ids, "Patients with diabetes")
```

---

## Methods

### `close()`

Closes the database connection. Always call this when you are done with the database, especially in scripts.

```python
db.close()
```

### `help()`

Prints a summary of available sub-objects and methods to stdout.

```python
db.help()
```

---

## Full Example

```python
import tspmdb

# 1. Open the database (create if new)
db = tspmdb.TspmDB("covid_study.sqlite3", destructive=True, parallel_threads=8, max_memory_mb=8192)

# 2. Define column name mapping from your CSV to TSPM's expected fields
col_names = {
    "PATIENT": "patient_id",      # column in CSV containing the patient identifier
    "DATE":    "obs_date",         # column in CSV containing the observation date
    "CODE":    "obs_code",         # column in CSV containing the observation code
    "TEXT":    "obs_description"   # column in CSV containing a human-readable description
}

# 3. Ingest data
db.dataset.ingest(
    "COVID_35k_subset.csv",
    col_names,
    zip_file="COVID_35k_subset.zip"
)

# 4. Calculate sequences and frequencies
db.dataset.calculate(sparsity_threshold=0.05)

# 5. Explore results
print(db.population.patients(as_list=True)[:5])
print(db.population.frequencies(as_pandas=True).head())

# 6. Close the database
db.close()
```

---

## Notes

- The `parallel_threads` parameter controls how many worker processes are spawned during `dataset.calculate()`. A good starting point is the number of physical CPU cores on your machine minus one.
- The `max_memory_mb` budget is divided equally across all worker processes. For large datasets (100k+ patients), allocating at least 8 GB total is recommended.
- Setting `destructive=True` allows the library to overwrite existing tables. This is useful during development but should be used carefully in production workflows.

