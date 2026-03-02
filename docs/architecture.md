# TSPM-DB Architecture Overview

## What is TSPM?

**Transitive Sequential Pattern Mining (TSPM)** is an algorithm for extracting temporal representations from Electronic Health Records (EHRs). It was introduced in the paper:

> Estiri et al., *Transitive Sequencing Medical Records for Mining Predictive and Interpretable Temporal Representations*, Patterns (2020). https://doi.org/10.1016/j.patter.2020.100051
> 
> Hügel et al., *tSPM+; a high-performance algorithm for mining transitive sequential patterns from clinical data*, Arxiv preprint (2023). https://doi.org/10.48550/arXiv.2309.05671

The core insight is that raw EHR observations do not directly reflect a patient's true health state — they reflect the clinical process, administrative workflows, and recording practices. TSPM addresses this by mining **transitive sequences**: ordered pairs of distinct observations `(obs_A → obs_B)` where the first occurrence of `obs_A` precedes the first occurrence of `obs_B` in a patient's record.

Unlike traditional Sequential Pattern Mining (SPM), which mines subsequences based on frequency thresholds, TSPM:
- Uses the **first occurrence** of each observation per patient (not all occurrences), reducing noise from repeated administrative entries.
- Mines **all transitive pairs** — if `A → B → C`, it captures `A→B`, `B→C`, *and* `A→C`.
- Applies a **sparsity filter** to retain only sequences that appear in at least a configurable percentage of patients.

The resulting sequences are used as features for downstream machine learning tasks such as disease classification and phenotype prediction.

---

## The TSPM-DB Library

**TSPM-DB** is a Python library that implements the TSPM algorithm on top of a local SQLite database. It is designed to be used interactively in Jupyter Notebooks by data scientists working with clinical EHR data.

The library handles:
1. **Ingesting** raw EHR data from CSV files (including ZIP-compressed files).
2. **Calculating** transitive sequences and population-level frequencies using parallel processing.
3. **Querying** patients, sequences, and frequencies — for the full population or for defined subpopulations.

---

## Database Schema

All data is stored in a single SQLite database file. The schema consists of six tables:

```
┌─────────────────────┐        ┌──────────────────────────┐
│   lookup_patients   │        │   lookup_observations    │
├─────────────────────┤        ├──────────────────────────┤
│ patient_num  (PK)   │        │ obs_code_id  (PK)        │
│ patient_id   (TEXT) │        │ obs_code     (TEXT)      │
└────────┬────────────┘        │ obs_description (TEXT)   │
         │                     └────────┬─────────────────┘
         │                              │
         ▼                              ▼
┌─────────────────────────────────────────────────────────┐
│                       source_data                       │
├─────────────────────────────────────────────────────────┤
│ patient_num  (FK → lookup_patients)                     │
│ obs_code     (FK → lookup_observations.obs_code_id)     │
│ obs_date     (DATE)                                     │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                        sequences                        │
├─────────────────────────────────────────────────────────┤
│ patient_num       (FK → lookup_patients)                │
│ obs_code_1        (FK → lookup_observations.obs_code_id)│
│ obs_code_2        (FK → lookup_observations.obs_code_id)│
│ temporal_distance (INTEGER, days between observations)  │
│ occurrence_count  (INTEGER)                             │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                       frequencies                       │
├─────────────────────────────────────────────────────────┤
│ obs_code_1        (FK → lookup_observations.obs_code_id)│
│ obs_code_2        (FK → lookup_observations.obs_code_id)│
│ temporal_distance (INTEGER)                             │
│ observation_cnt   (INTEGER, total occurrences)          │
│ patient_cnt       (INTEGER, distinct patients)          │
└─────────────────────────────────────────────────────────┘

┌──────────────────────┐     ┌──────────────────────────────┐
│    subpopulations    │     │   subpopulation_patients     │
├──────────────────────┤     ├──────────────────────────────┤
│ subpop_num  (PK)     │     │ subpop_num  (FK)             │
│ subpop_id   (TEXT)   │     │ patient_num (FK)             │
│ description (TEXT)   │     └──────────────────────────────┘
└──────────────────────┘
```

### ID Translation Pattern

The library uses a consistent pattern of **human-readable string identifiers** mapped to **internal integer keys**:

| Human-readable | Internal key | Table |
|---|---|---|
| `patient_id` (string) | `patient_num` (integer) | `lookup_patients` |
| `obs_code` (string) | `obs_code_id` (integer) | `lookup_observations` |
| `subpop_id` (string) | `subpop_num` (integer) | `subpopulations` |

By default, all API methods return and accept human-readable string identifiers. Pass `with_ids=True` or `no_id_translation=True` to work with raw integer keys instead.

---

## Object Model

```
TspmDB
├── .dataset          → Dataset
│     ├── .ingest()
│     ├── .calculate()
│     └── .clear()
│
├── .population       → Population
│     ├── .patients()       → list[PatientInstance] | list | DataFrame
│     ├── .sequences()      → list | DataFrame | iterator
│     └── .frequencies()    → list | DataFrame | iterator
│
└── .subpopulation    → Subpopulation
      ├── .list()
      ├── .create()         → SubpopulationInstance
      └── .get()            → SubpopulationInstance
            ├── .patients   → SubpopulationInstancePatients
            │     ├── .list()
            │     ├── .add()
            │     └── .remove()
            └── .sequences  → SubpopulationInstanceSequences
                  └── .frequencies()  → list | DataFrame | iterator

PatientInstance
├── .id               → str  (patient_id)
├── .events()         → list | DataFrame
└── .sequences()      → list | DataFrame | iterator
```

---

## Sequence Calculation Pipeline

The `dataset.calculate()` method runs a multi-step parallel pipeline:

1. **Step 1 — Generate sequences per patient** (`worker_SequenceGeneration_Step_1`):
   Each worker process receives a batch of `patient_num` values. For each patient, it computes all transitive pairs `(obs_code_1, obs_code_2, temporal_distance)` from `source_data`, using the first occurrence of each observation. Results are written to a per-process temporary SQLite database.

2. **Step 2 — Aggregate frequencies** (`worker_SequenceGeneration_Step_2`):
   Each worker reads its temporary database and aggregates `observation_cnt` (SUM) and `patient_cnt` (COUNT DISTINCT) per `(obs_code_1, obs_code_2, temporal_distance)` combination, writing results into the main `frequencies` table using an UPSERT.

3. **Sparsity filter**:
   Sequences where `patient_cnt / total_patients < sparsity_threshold` are deleted from `frequencies`. The default threshold is `0.05` (5%).

4. **Step 3 — Copy filtered sequences** (`worker_SequenceGeneration_Step_3`):
   Each worker copies only the sequences that survived the sparsity filter from its temporary database into the main `sequences` table.

5. **Index creation**: An index on `(obs_code_1, obs_code_2, temporal_distance)` is created on the `sequences` table for fast querying.

Temporal buckets can optionally be passed to `calculate()` to group `temporal_distance` values into discrete ranges (e.g., 0–1 days, 1–3 days, 3–7 days).

