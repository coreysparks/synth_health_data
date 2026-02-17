# Synthetic Inpatient Claims Data

A self-contained dataset and PostgreSQL database for practicing Medicaid
inpatient claims analysis. The data is entirely synthetic — generated from a
configurable simulation — and is designed to mimic the messiness found in
real-world administrative claims files.

---

## Purpose

Real claims data is hard to share for training and experimentation. This repo
provides a realistic stand-in: a population of synthetic members with inpatient
stays, deliberately noisy claims representing those stays, and a clean analytic
fact table built from that noise. It is useful for:

- Learning claims-based encounter analysis (stay identification, grouping)
- Practicing EDA on Medicaid-style data in R or Python
- Testing ETL pipelines and data quality checks
- Demonstrating how messy raw claims translate into analytic tables

---

## Data generation

`code/make synth data.py` creates the full dataset from scratch.

**Simulation parameters (defaults):**

| Parameter | Value |
|---|---|
| Members | 10,000 |
| Time window | 2022-01-01 through 2023-12-31 (24 months) |
| State | Arkansas (`STATE_CD = AR`, `SUBMTG_STATE_CD = 05`) |
| Probability of any stay | 25% of members |
| Stays per member | 1–2 |
| Length of stay | 1–7 days |
| Random seed | 7 |

**Messiness intentionally injected into raw claims:**

| Data quality issue | Rate |
|---|---|
| Stay split into interim claims | 55% of stays |
| Header date inconsistency | 8% of claims |
| Line date out of header bounds | 6% of claim lines |
| Transfer/overlap between claims | 10% of stays ≥ 4 days |
| Duplicate header row | 2% of claims |
| Duplicate line row | 1.5% of claim lines |
| Denied claim | 5% of claims |

**Clinical code pools used:**

- DRG codes: 194, 291, 392, 470, 690, 775
- Diagnosis codes (ICD-10): E11.9 (diabetes), I10 (hypertension), F32.9
  (depression), J18.9 (pneumonia), M54.5 (low back pain)
- Revenue center codes: routine (01xx), ICU (020x), pharmacy (0250), lab
  (0300), ER (0450)

---

## Database

The generated CSVs are loaded into a local PostgreSQL instance via
`code/load script.sql`.

**Connection:**

```
host:     localhost
port:     5433
database: postgres
schema:   synth
user:     postgres
```

**Tables:**

| Table | Description |
|---|---|
| `fact_inpatient_stay` | Analytic fact table — one row per resolved inpatient stay |
| `ip_claim_header_canonical` | Deduplicated claim headers |
| `ip_claim_line_canonical` | Deduplicated claim lines |
| `ip_claim_header_messy` | Raw claim headers including duplicates and errors |
| `ip_claim_line_messy` | Raw claim lines including duplicates and errors |
| `true_stays_reference` | Ground truth stays used to generate the claims (for validation) |
| `person_msis_xwalk` | Person ID to MSIS ID crosswalk |

The `fact_inpatient_stay` table is built by grouping canonicalized claim headers
into stays (consecutive claims with ≤ 1-day gap), then joining ICU line detail
to compute `icu_days`.

---

## Code

| File | Purpose |
|---|---|
| `code/make synth data.py` | Generate all CSVs in `output/` |
| `code/load script.sql` | Load CSVs into the `synth` schema |
| `code/basic_query.R` | Quick R connection and query scratch file |
| `connecting_to_synth_db.qmd` | Quarto document: R and Python connection guide with basic EDA |

---

## Quarto document

`connecting_to_synth_db.qmd` demonstrates how to connect to the database and
explore `fact_inpatient_stay` in both R and Python, with each approach shown
side-by-side in a tabset. Render it with:

```bash
quarto render connecting_to_synth_db.qmd
```

**R packages required:** `DBI`, `RPostgres`, `dplyr`, `dbplyr`

**Python packages required:** `sqlalchemy`, `psycopg2-binary`, `pandas`
