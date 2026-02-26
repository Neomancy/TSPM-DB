# API Reference: `PatientInstance`

A `PatientInstance` represents a single patient in the database. Instances are returned by `db.population.patients()` (default mode) and can also be constructed directly when you have a `patient_num`.

---

## Constructor

```python
PatientInstance(tspmdb_ref, patient_num: int)
```

Looks up the patient in the `lookup_patients` table and caches their `patient_id`. Raises `KeyError` if the `patient_num` does not exist.

| Parameter | Type | Description |
|---|---|---|
| `tspmdb_ref` | `TspmDB` | Reference to the parent `TspmDB` instance. |
| `patient_num` | `int` | The internal integer database key for the patient. |

```python
# Typically obtained from db.population.patients()
patients = db.population.patients()
patient = patients[0]

# Or constructed directly using a patient_num
patient = PatientInstance(db, 42)
```

---

## Properties

### `id`

```python
patient.id  # -> str
```

Read-only. Returns the `patient_id` string for this patient (e.g., `"PT001"`).

```python
print(patient.id)  # "PT001"
```

---

## Methods

### `events()`

```python
patient.events(as_pandas: bool = False)
```

Returns all observation events for this patient from the `source_data` table. Observation code IDs are translated to their string codes and descriptions via `lookup_observations`.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `as_pandas` | `bool` | `False` | If `True`, returns a Pandas DataFrame. |

**Return columns:**

| Column | Type | Description |
|---|---|---|
| `obs_code` | `str` | The observation code string (e.g., ICD-10 or RxNorm code). |
| `obs_description` | `str` | Human-readable description of the observation. |
| `obs_date` | `str` | Date the observation was recorded. |

Results are ordered by `obs_date` ascending.

```python
# List of dicts
events = patient.events()
for e in events:
    print(e["obs_date"], e["obs_code"], e["obs_description"])

# Pandas DataFrame
events_df = patient.events(as_pandas=True)
events_df.head()
```

---

### `sequences()`

```python
patient.sequences(as_pandas: bool = False, as_iterator: bool = False)
```

Returns all pre-calculated transitive sequences for this patient from the `sequences` table. Observation code IDs are translated to their string codes.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `as_pandas` | `bool` | `False` | If `True`, returns a Pandas DataFrame. |
| `as_iterator` | `bool` | `False` | If `True`, returns a generator yielding one dictionary per row. |

**Return columns:**

| Column | Type | Description |
|---|---|---|
| `patient_id` | `str` | The patient's string identifier. |
| `obs_code_1` | `str` | The first observation code in the sequence. |
| `obs_code_2` | `str` | The second observation code in the sequence. |
| `time_diff` | `int` | Temporal distance in days (or bucket number). |
| `occurrence_count` | `int` | Number of times this sequence was observed for this patient. |

```python
# List of dicts
seqs = patient.sequences()
for s in seqs:
    print(s["obs_code_1"], "→", s["obs_code_2"], f"({s['time_diff']} days)")

# Pandas DataFrame
seqs_df = patient.sequences(as_pandas=True)
seqs_df.head()

# Generator (memory-efficient)
for s in patient.sequences(as_iterator=True):
    print(s)
```

---

## Full Example

```python
import tspmdb

db = tspmdb.TspmDB("covid_study.sqlite3")

# Get all patients as PatientInstance objects
patients = db.population.patients()

# Inspect the first patient
p = patients[0]
print("Patient ID:", p.id)

# View their observation history
events_df = p.events(as_pandas=True)
print(f"Total events: {len(events_df)}")
print(events_df.head())

# View their sequences
seqs_df = p.sequences(as_pandas=True)
print(f"Total sequences: {len(seqs_df)}")
print(seqs_df.sort_values("time_diff").head(10))

db.close()
```

---

## Notes

- `PatientInstance` objects are lightweight — they only store the `patient_num` and cached `patient_id`. All data is fetched from the database on demand when `events()` or `sequences()` is called.
- If you need to work with many patients at once, prefer `db.population.sequences()` or `db.population.frequencies()` which operate over the full population in a single query, rather than iterating over individual `PatientInstance` objects.
- The `sequences()` method returns data from the pre-calculated `sequences` table. If `dataset.calculate()` has not been run, this will return empty results.

