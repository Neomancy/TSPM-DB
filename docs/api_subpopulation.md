# API Reference: Subpopulations

Subpopulations allow you to define named cohorts of patients within the database and query their sequences and frequencies independently. The subpopulation API is spread across four classes that are accessed through a natural hierarchy.

---

## `Subpopulation` — `db.subpopulation`

The top-level subpopulation manager, accessed via `db.subpopulation`.

### `list()`

```python
db.subpopulation.list(as_pandas: bool = False)
```

Returns all subpopulations defined in the database.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `as_pandas` | `bool` | `False` | If `True`, returns a Pandas DataFrame. |

**Return columns:** `subpop_id`, `description`

```python
# List of dicts
subpops = db.subpopulation.list()

# As DataFrame
df = db.subpopulation.list(as_pandas=True)
```

### `create()`

```python
db.subpopulation.create(subpop_id: str, patient_ids: list, description: str = "") -> SubpopulationInstance
```

Creates a new subpopulation and optionally populates it with patients. Returns a `SubpopulationInstance`.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `subpop_id` | `str` | *(required)* | Unique string identifier for the subpopulation. |
| `patient_ids` | `list[str]` | *(required)* | List of `patient_id` strings to add. Pass `[]` to create an empty subpopulation. |
| `description` | `str` | `""` | Optional human-readable description. |

```python
cohort = db.subpopulation.create(
    "heart_failure_patients",
    ["PT001", "PT002", "PT003"],
    "Patients with a CHF diagnosis"
)
```

### `get()`

```python
db.subpopulation.get(subpop_id: str) -> SubpopulationInstance
```

Retrieves an existing subpopulation by its string ID. Raises `KeyError` if not found.

```python
cohort = db.subpopulation.get("heart_failure_patients")
```

---

## `SubpopulationInstance`

Returned by `create()` and `get()`. Represents a single subpopulation and provides access to its patients and sequences via two sub-objects.

### Properties

| Property | Type | Description |
|---|---|---|
| `.patients` | `SubpopulationInstancePatients` | Manage the patients in this subpopulation. |
| `.sequences` | `SubpopulationInstanceSequences` | Query sequences and frequencies for this subpopulation. |

---

## `SubpopulationInstancePatients` — `cohort.patients`

Manages the patient membership of a subpopulation.

### `list()`

```python
cohort.patients.list(as_pandas: bool = False, no_id_translation: bool = False)
```

Returns the patients in this subpopulation.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `as_pandas` | `bool` | `False` | If `True`, returns a Pandas DataFrame. |
| `no_id_translation` | `bool` | `False` | If `True`, returns raw `patient_num` integers instead of `patient_id` strings. |

```python
# List of patient_id strings
ids = cohort.patients.list()

# List of patient_num integers
nums = cohort.patients.list(no_id_translation=True)

# As DataFrame
df = cohort.patients.list(as_pandas=True)
```

### `add()`

```python
cohort.patients.add(patient_ids, no_id_translation: bool = False)
```

Adds one or more patients to the subpopulation.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `patient_ids` | `str`, `int`, or `list` | *(required)* | A single `patient_id` string, a single `patient_num` integer, or a list of either. |
| `no_id_translation` | `bool` | `False` | If `True`, treats input as `patient_num` integers. Raises `KeyError` if a `patient_num` does not exist. |

```python
# Add a single patient by patient_id
cohort.patients.add("PT004")

# Add multiple patients
cohort.patients.add(["PT005", "PT006", "PT007"])

# Add by patient_num (raw integer key)
cohort.patients.add(42, no_id_translation=True)
```

### `remove()`

```python
cohort.patients.remove(patient_ids, no_id_translation: bool = False)
```

Removes one or more patients from the subpopulation. If a `patient_id` does not exist in the lookup table, it is silently skipped. If `no_id_translation=True` and a `patient_num` does not exist, a `KeyError` is raised.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `patient_ids` | `str`, `int`, or `list` | *(required)* | A single `patient_id` string, a single `patient_num` integer, or a list of either. |
| `no_id_translation` | `bool` | `False` | If `True`, treats input as `patient_num` integers. |

```python
# Remove a single patient
cohort.patients.remove("PT004")

# Remove multiple patients
cohort.patients.remove(["PT005", "PT006"])
```

---

## `SubpopulationInstanceSequences` — `cohort.sequences`

Queries sequence-derived statistics for this subpopulation. Unlike `db.population.frequencies()` which reads from the pre-calculated `frequencies` table, this class **calculates frequencies on-the-fly** from the `sequences` table, filtered to only the patients in this subpopulation.

### `frequencies()`

```python
cohort.sequences.frequencies(
    observation1=None,
    observation2=None,
    as_pandas: bool = False,
    as_iterator: bool = False,
    with_ids: bool = False
)
```

Returns frequency statistics for this subpopulation, calculated dynamically from the `sequences` table.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `observation1` | `str` or `list[str]` | `None` | Filter by `obs_code_1`. Single string or list. If `None`, all values included. |
| `observation2` | `str` or `list[str]` | `None` | Filter by `obs_code_2`. Single string or list. If `None`, all values included. |
| `as_pandas` | `bool` | `False` | If `True`, returns a Pandas DataFrame. |
| `as_iterator` | `bool` | `False` | If `True`, returns a generator. |
| `with_ids` | `bool` | `False` | If `True`, returns raw integer `obs_code_id` values instead of string codes. |

**Aggregation logic:**
- `observation_cnt` = `SUM(occurrence_count)` across all patients in the subpopulation for each `(obs_code_1, obs_code_2, temporal_distance)` combination.
- `patient_cnt` = `COUNT(DISTINCT patient_num)` for each combination.

**Return columns:** `obs_code_1`, `obs_code_2`, `temporal_distance`, `observation_cnt`, `patient_cnt`

All observation codes are validated upfront. A `KeyError` is raised for any code not found in `lookup_observations`.

```python
# All frequencies for this subpopulation
df = cohort.sequences.frequencies(as_pandas=True)

# Filter by a specific obs_code_1
df = cohort.sequences.frequencies(observation1="428.0", as_pandas=True)

# Filter by multiple codes (OR within list, AND between parameters)
df = cohort.sequences.frequencies(
    observation1=["428.0", "250.00"],
    observation2="metoprolol",
    as_pandas=True
)

# Memory-efficient iterator
for freq in cohort.sequences.frequencies(as_iterator=True):
    print(freq)
```

---

## Full Subpopulation Workflow Example

```python
import tspmdb

db = tspmdb.TspmDB("covid_study.sqlite3")

# Create a subpopulation
chf_cohort = db.subpopulation.create(
    "chf_patients",
    ["PT001", "PT002", "PT003"],
    "Patients with congestive heart failure"
)

# Add more patients later
chf_cohort.patients.add(["PT010", "PT011"])

# Remove a patient
chf_cohort.patients.remove("PT001")

# List current members
print(chf_cohort.patients.list())

# Query frequencies for this cohort
df = chf_cohort.sequences.frequencies(as_pandas=True)
print(df.sort_values("patient_cnt", ascending=False).head(10))

# Retrieve the subpopulation later in another session
db2 = tspmdb.TspmDB("covid_study.sqlite3")
chf_cohort2 = db2.subpopulation.get("chf_patients")
print(chf_cohort2.patients.list())

db.close()
db2.close()
```

