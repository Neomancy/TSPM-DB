# API Reference: `Population`

The `Population` object is accessed via `db.population`. It provides methods for querying all patients, sequences, and frequencies across the entire database — i.e., the full study population.

---

## `patients()`

```python
db.population.patients(
    as_list: bool = False,
    as_pandas: bool = False,
    with_ids: bool = False
)
```

Returns all patients in the database.

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `as_list` | `bool` | `False` | If `True`, returns a list of `patient_id` strings instead of `PatientInstance` objects. |
| `as_pandas` | `bool` | `False` | If `True`, returns a Pandas DataFrame. |
| `with_ids` | `bool` | `False` | If `True`, includes the internal `patient_db_num` integer alongside `patient_id`. Only applies when `as_list=True` or `as_pandas=True`. |

### Return Values

| `as_list` | `as_pandas` | `with_ids` | Return type |
|---|---|---|---|
| `False` | `False` | — | `list[PatientInstance]` |
| `True` | `False` | `False` | `list[str]` — patient_id strings |
| `True` | `False` | `True` | `list[dict]` — `{"patient_id": str, "patient_db_num": int}` |
| `False` | `True` | `False` | `DataFrame` with column `patient_id` |
| `False` | `True` | `True` | `DataFrame` with columns `patient_id`, `patient_db_num` |

### Examples

```python
# Default: list of PatientInstance objects
patients = db.population.patients()
for p in patients[:3]:
    print(p.id)

# Simple list of patient ID strings
patient_ids = db.population.patients(as_list=True)

# Pandas DataFrame with both identifiers
df = db.population.patients(as_pandas=True, with_ids=True)
df.head()
```

---

## `sequences()`

```python
db.population.sequences(
    as_pandas: bool = False,
    as_iterator: bool = False
)
```

Returns all pre-calculated transitive sequences from the `sequences` table for the full population. Observation code IDs are translated to their string codes.

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `as_pandas` | `bool` | `False` | If `True`, returns a Pandas DataFrame. |
| `as_iterator` | `bool` | `False` | If `True`, returns a generator that yields one dictionary per row. Useful for large result sets to avoid loading everything into memory. |

### Return Columns

| Column | Type | Description |
|---|---|---|
| `patient_id` | `str` | The patient's string identifier. |
| `obs_code_1` | `str` | The first observation code in the sequence. |
| `obs_code_2` | `str` | The second observation code in the sequence. |
| `time_diff` | `int` | Temporal distance in days (or bucket number if buckets were used). |
| `occurrence_count` | `int` | Number of times this sequence was observed for this patient. |

### Examples

```python
# List of dicts
seqs = db.population.sequences()

# Pandas DataFrame
seqs_df = db.population.sequences(as_pandas=True)
seqs_df.head()

# Memory-efficient iterator for large datasets
for seq in db.population.sequences(as_iterator=True):
    print(seq["patient_id"], seq["obs_code_1"], "→", seq["obs_code_2"])
```

---

## `frequencies()`

```python
db.population.frequencies(
    observation1=None,
    observation2=None,
    as_pandas: bool = False,
    as_iterator: bool = False,
    with_ids: bool = False
)
```

Returns population-level frequency statistics from the pre-calculated `frequencies` table. Results can be filtered by observation codes.

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `observation1` | `str` or `list[str]` | `None` | Filter by `obs_code_1`. A single string or a list of strings. If `None`, all values are included. |
| `observation2` | `str` or `list[str]` | `None` | Filter by `obs_code_2`. A single string or a list of strings. If `None`, all values are included. |
| `as_pandas` | `bool` | `False` | If `True`, returns a Pandas DataFrame. |
| `as_iterator` | `bool` | `False` | If `True`, returns a generator yielding one dictionary per row. |
| `with_ids` | `bool` | `False` | If `True`, returns raw integer `obs_code_id` values instead of translated string codes. |

### Filter Logic

- Parameters are **AND**-ed: passing both `observation1` and `observation2` returns only rows matching both.
- When a parameter is a **list**, the values within it are **OR**-ed (implemented as SQL `IN`).
- All observation codes are **validated upfront**. A `KeyError` is raised immediately if any code does not exist in `lookup_observations`, with the invalid code(s) named in the error message.
- If filters are valid but no matching rows exist, an empty list or DataFrame is returned (not an error).

### Return Columns

| Column | Type | Description |
|---|---|---|
| `obs_code_1` | `str` or `int` | First observation code (string by default; integer if `with_ids=True`). |
| `obs_code_2` | `str` or `int` | Second observation code (string by default; integer if `with_ids=True`). |
| `temporal_distance` | `int` | Days between the two observations (or bucket number). |
| `observation_cnt` | `int` | Total number of times this sequence was observed across all patients. |
| `patient_cnt` | `int` | Number of distinct patients who have this sequence. |

### Examples

```python
# All frequencies as a DataFrame
df = db.population.frequencies(as_pandas=True)

# Filter by a single obs_code_1
df = db.population.frequencies(observation1="428.0", as_pandas=True)

# Filter by multiple obs_code_1 values (OR logic within the list)
df = db.population.frequencies(observation1=["428.0", "250.00"], as_pandas=True)

# Filter by both obs_code_1 AND obs_code_2
df = db.population.frequencies(observation1="428.0", observation2="metoprolol", as_pandas=True)

# Memory-efficient iterator
for freq in db.population.frequencies(as_iterator=True):
    print(freq["obs_code_1"], "→", freq["obs_code_2"], ":", freq["patient_cnt"], "patients")

# Return raw integer IDs instead of string codes
df = db.population.frequencies(with_ids=True, as_pandas=True)

# KeyError example — invalid code raises immediately
try:
    db.population.frequencies(observation1="INVALID_CODE")
except KeyError as e:
    print(e)  # "Observation code(s) not found in observation1: INVALID_CODE"
```

---

## `help()`

```python
db.population.help()
```

Prints a summary of available methods to stdout.

