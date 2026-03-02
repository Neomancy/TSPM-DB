# TSPM-DB: Transitive Sequential Pattern Mining Database

A Python library for mining temporal patterns from Electronic Health Records (EHRs) using the **Transitive Sequential Pattern Mining (TSPM)** algorithm. TSPM-DB provides an efficient, multiprocessing implementation built on SQLite for analyzing large-scale clinical datasets.

## Overview

**TSPM** is an algorithm for extracting meaningful temporal representations from EHR data by mining **transitive sequences**—ordered pairs of distinct observations where the first occurrence of observation A precedes the first occurrence of observation B in a patient's record.

Unlike traditional Sequential Pattern Mining, TSPM:
- Uses the **first occurrence** of each observation per patient (reducing noise from repeated administrative entries)
- Mines **all transitive pairs** (if A → B → C, it captures A→B, B→C, *and* A→C)
- Applies a **sparsity filter** to retain only sequences appearing in a configurable percentage of patients

The resulting sequences serve as features for downstream machine learning tasks such as disease classification and phenotype prediction.

**References:** 
> Estiri et al., *Transitive Sequencing Medical Records for Mining Predictive and Interpretable Temporal Representations*, Patterns (2020). https://doi.org/10.1016/j.patter.2020.100051
> 
> Hügel et al., *tSPM+; a high-performance algorithm for mining transitive sequential patterns from clinical data*, Arxiv preprint (2023). https://doi.org/10.48550/arXiv.2309.05671



## Features

- ✅ **Efficient multiprocessing** — Parallel sequence generation and aggregation across CPU cores
- ✅ **SQLite-based** — Single-file database, no external dependencies
- ✅ **Temporal bucketing** — Group temporal distances into clinically meaningful ranges
- ✅ **Subpopulation support** — Define and compare patient cohorts independently
- ✅ **Flexible querying** — Filter sequences and frequencies by observation codes
- ✅ **Pandas integration** — Return results as DataFrames or iterators for memory efficiency
- ✅ **Jupyter-friendly** — Designed for interactive analysis in notebooks

## Installation

### From Source

```bash
git clone https://github.com/hackerceo/tspmdb.git
cd tspmdb
pip install -e .
```

### Requirements

- Python 3.9+
- pandas
- sqlite3 (included with Python)

## Quick Start

### 1. Open a Database

```python
import tspmdb

db = tspmdb.TspmDB("my_study.sqlite3", parallel_threads=4, max_memory_mb=4096)
```

### 2. Ingest EHR Data

```python
col_names = {
    "PATIENT": "patient_id",      # Column with patient identifier
    "DATE":    "obs_date",         # Column with observation date
    "CODE":    "obs_code",         # Column with observation code
    "TEXT":    "obs_description"   # Column with description (optional)
}

db.dataset.ingest(
    "observations.csv",
    col_names,
    zip_file="observations.zip"  # Optional: read from ZIP archive
)
```

### 3. Calculate Sequences

```python
db.dataset.calculate(
    temporal_buckets=[(0, 1), (1, 7), (7, 30), (30, 365)],
    sparsity_threshold=0.05  # Keep sequences in ≥5% of patients
)
```

### 4. Query Results

```python
# Get all patients
patients = db.population.patients(as_list=True)

# Get population-level frequencies
freq_df = db.population.frequencies(as_pandas=True)
freq_df.sort_values("patient_cnt", ascending=False).head(10)

# Create a subpopulation and query it
cohort = db.subpopulation.create("diabetic_patients", patient_ids, "Patients with diabetes")
cohort_freq = cohort.sequences.frequencies(as_pandas=True)

db.close()
```

## Documentation

Comprehensive documentation is available in the `docs/` folder:

| Document | Purpose |
|---|---|
| **[architecture.md](docs/architecture.md)** | TSPM algorithm overview, database schema, object model |
| **[api_tspmdb.md](docs/api_tspmdb.md)** | `TspmDB` class reference |
| **[api_dataset.md](docs/api_dataset.md)** | `Dataset` class reference (ingest, calculate) |
| **[api_population.md](docs/api_population.md)** | `Population` class reference (patients, sequences, frequencies) |
| **[api_subpopulation.md](docs/api_subpopulation.md)** | Subpopulation management and querying |
| **[api_patient_instance.md](docs/api_patient_instance.md)** | `PatientInstance` class reference |
| **[AI_Use.md](docs/AI_Use.md)** | Notes on AI use in development and validation |

## Jupyter Notebooks

Three example notebooks demonstrate common workflows:

1. **[01_getting_started.ipynb](docs/01_getting_started.ipynb)** — Installation, data ingestion, sequence calculation
2. **[02_exploring_population.ipynb](docs/02_exploring_population.ipynb)** — Querying patients, sequences, and frequencies
3. **[03_subpopulations.ipynb](docs/03_subpopulations.ipynb)** — Creating cohorts and comparing frequency profiles

## Example Dataset

The repository includes a 35,000-patient synthetic COVID-19 EHR dataset (`100k-COVID_data/COVID_35k_subset.zip`) for testing and demonstration.

## Architecture

TSPM-DB consists of:

- **Core Algorithm** — Hand-built TSPM implementation with parallel sequence generation
- **SQLite Database** — Six-table schema for patients, observations, sequences, frequencies, and subpopulations
- **Python API** — Object-oriented interface for data ingestion, calculation, and querying
- **Worker Processes** — Multiprocessing pipeline for efficient large-scale computation

See [docs/architecture.md](docs/architecture.md) for detailed schema and pipeline information.

## Testing

Run the test suite with pytest:

```bash
pytest tests/ -v
```

Key test files:
- `tests/test_tspmdb.py` — Core database and ingestion tests
- `tests/test_population.py` — Population querying tests
- `tests/test_subpopulations.py` — Subpopulation management tests
- `tests/test_patient.py` — Individual patient tests

## License

This project is licensed under the **GNU Affero General Public License v3.0** (AGPL-3.0). See [LICENSE](LICENSE) for details.

## Citation

If you use TSPM-DB in your research, please cite the original TSPM paper:

```bibtex
@article{estiri2020transitive,
  title={Transitive Sequencing Medical Records for Mining Predictive and Interpretable Temporal Representations},
  author={Estiri, Hossein and Strasser, Zachary H and Brat, Gabriel A and Sinha, Usha},
  journal={Patterns},
  volume={1},
  number={5},
  pages={100051},
  year={2020},
  publisher={Elsevier}
}
```

## Contributing

Contributions are welcome! Please ensure:
- Code follows the existing style
- All tests pass (`pytest tests/ -v`)
- New features include tests and documentation

## Support

For issues, questions, or suggestions, please open an issue on [GitHub](https://github.com/hackerceo/tspmdb/issues).

## Authors

- **Nick Benik** (Neomancy Inc / Harvard Medical School) — Algorithm design, API architecture, core implementation
- **Independent Review** — Mr. J.H., PhD (Visiting Researcher) and Mr. H.E., PhD (Institution XYZ)

See [docs/AI_Use.md](docs/AI_Use.md) for details on development approach and validation.