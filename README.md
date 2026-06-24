# Maternity Services Statistics to CML Schema Conversion

## Overview

This pipeline converts the **Maternity Services Monthly Statistics** into the format required by the **NHS England Central Metrics Library (CML)**.

The source data is the Official Statistics about NHS-funded maternity services in England, drawn from the Maternity Services Data Set (MSDS). It covers activity at the booking appointment, during pregnancy, during and after birth, and information on pregnancy outcomes.

The pipeline takes the MSDS data in a tidy (long) format, applies a series of configurable transformations using **pandas**, and produces two output tables matching the CML v2.0 schema: a **metric table** and a **dimensions table**.

---

## Prerequisites

- Python >= 3.10

---

## Getting Started

### Installation

#### Outside RDS4, or in RDS4 but using the internal package repository

If you're in RDS4, remember to configure pip to look at the internal package repository (see guidance on Confluence).

```bash
python -m venv .venv
source .venv/bin/activate
pip install .
```

#### In RDS4 - to use local clones of the CML repos

First clone this project and the two repos to the same folder as this project is in:

```bash
git clone https://<user_name>:<token>@nhsd-git.digital.nhs.uk/data-services/analytics-service/cml/msds-monthly-to-cml.git
git clone https://<user_name>:<token>@nhsd-git.digital.nhs.uk/data-services/analytics-service/cml/cml-conversion-helpers.git
git clone https://<user_name>:<token>@nhsd-git.digital.nhs.uk/data-services/analytics-service/cml/cml-schemas.git
```

So your folder will look something like this:

```
my_projects/
|---msds-monthly-to-cml/ 
|---cml-schemas/
|---cml-conversion-helpers
```

Then install the packages:

```bash
cd msds-monthly-to-cml
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-rds4.txt
```

#### For development (editable install with test dependencies):

```bash
pip install -e ".[dev]"
```

This installs pytest.

### Setup

#### Config

Rename the config-example.yaml file to config.yaml, and then update it as needed (see the section below).

You need to do this because config.yaml is in the .gitignore, meaning you can change the settings without git tracking the changes.

#### .env

Create a file called .env (use VS Code - Windows can be a bit fussy about creating files that are only extensions).

This file needs to contain the name of the SQL Server that contains the reference data under the variable name `SERVER`, e.g.:

```bash
SERVER="<name of server>"
```

### Running the pipeline

OK, now you should be ready to go! Just run:

```bash
python create_cml_tables.py
```

### Running the tests

```bash
pytest
```

---

## Configuration

All pipeline parameters are defined in [`config.yaml`](config.yaml). You should update this file for each run.

Key fields:

| Field | Description |
|-------|-------------|
| `publication_date` | Publication date for the output (used as a literal column value) |
| `last_ingest_timestamp` | Timestamp of the last data ingest |
| `path_to_source_data` | Path to the input CSV file |
| `output_dir` | Directory where output CSVs are written |
| `log_dir` | Directory where log files are written |
| `dimensions` | List of all dimension names present in the source data |
| `dimension_creation_exclusions` | Dimensions to exclude from the output dimensions table (e.g. `mbrrace_grouping`, which is handled as a separate column) |
| `processing_funcs` | Ordered list of transformation functions to apply, each with a `name` and `params` |

### YAML anchors

`config.yaml` uses YAML anchors (`&`) and aliases (`*`) to avoid repeating values:

```yaml
publication_date: &publication_date "01/12/2026"
last_ingest_timestamp: &last_ingest_timestamp "15/12/2026"
```

These can then be referenced elsewhere in the file with `*publication_date` and `*last_ingest_timestamp`.

---

## Input Format

The pipeline expects a **tidy (long) format** CSV, where each row represents a single metric value for a given dimension/attribute combination:

| Org_Code | Org_Level | Dimension | Attribute | Final_value | ReportingPeriodStartDate | ReportingPeriodEndDate |
|----------|-----------|-----------|-----------|-------------|--------------------------|------------------------|
| RXX | Trust | EthnicCategoryMotherGroup | EthnicWhite | 82 | 01/04/2026 | 30/06/2026 |
| RXX | Trust | AgeAtBookingMotherGroup | Age25to29 | 54 | 01/04/2026 | 30/06/2026 |
| ALL | England | EthnicCategoryMotherGroup | EthnicWhite | 79 | 01/04/2026 | 30/06/2026 |

The `Dimension` column identifies which dimension the row belongs to, and the attribute value (e.g. `EthnicWhite`) sits in the `Attribute` column.

---

## Output Format

Two CSVs are written to wherever you specify in the config - you can use the `data_out` folder, or somewhere else if you want:

### Metric table (`data_out/metric.csv`)

One row per data point, containing the numeric value and metadata:

| datapoint_id | metric_id | metric_dimension_id | location_id | location_type | metric_value | reporting_period_start_datetime | reporting_grain | last_record_timestamp | publication_datetime | last_ingest_timestamp | additional_metric_values |
|---|---|---|---|---|---|---|---|---|---|---|---|

### Dimensions table (`data_out/dimensions.csv`)

One row per data point, one column per dimension. Each dimension column defaults to `all_<dimension>` unless the data point belongs to that dimension:

| datapoint_id | metric_dimension_id | dimension_id | dimension_type_id | dimension_count | EthnicCategoryMotherGroup | AgeAtBookingMotherGroup | ... |
|---|---|---|---|---|---|---|---|

The `dimension_id` is an MD5 hash of the dimension column values and links the metric and dimensions tables together.

---

## Pipeline Steps

The transformation logic is defined as an ordered sequence in `config.yaml` under `processing_funcs`. Each entry maps to a registered function in the `cml_conversion_helpers` library via `PROCESSING_FUNC_REGISTRY`.

The steps applied in this pipeline are:

1. **`move_attributes_to_new_dimension`** — moves MBRRACE grouping values (e.g. `"Group 1. Level 3 NICU & NS"`) out of `Org_Code` into a new `mbrrace_grouping` column
2. **`replace_col_values`** — replaces `"ALL"` in `Org_Code` with `"england"`, and maps `Org_Level` values to CML location types (e.g. `Provider` → `nhs-trust`)
3. **`rename_cols`** — renames source columns to CML schema names (`Org_Code` → `location_id`, etc.)
4. **`cast_date_col_to_timestamp`** — casts date string columns to timestamps
5. **`add_lit_col`** — adds `reporting_grain` as `"monthly"`
6. **`create_uuid_col`** — generates a unique `datapoint_id` per row
7. **`concat_cols`** — builds `metric_id` by concatenating `Dimension` and `Count_Of`
8. **`add_lit_col`** + **`cast_date_col_to_timestamp`** — adds `publication_datetime` and `last_ingest_timestamp` as typed columns
9. **`add_lit_col`** — adds `additional_metric_values` as a null column

You don't have to use this config-driven approach if you don't want to. You can simply add your pandas code into the `create_cml_tables.py` file.

After these config-driven steps, the pipeline builds the dimension columns (`create_dimension_columns`, `create_dimension_type_col`, `create_dimension_count_col`, `create_md5_hash_col`) and assembles the `datapoint_id` and `metric_dimension_id` via `concat_cols`.

See the [`cml_conversion_helpers` API reference](#api-reference) below for full details on each function.

---

## Project Structure

```
├── create_cml_tables.py          <- Entry point — runs the full pipeline
├── config.yaml                   <- Pipeline parameters and processing steps
│
├── src/
│   └── msds_monthly_to_cml/
│       ├── data_ingestion/
│       │   ├── get_data.py       <- Utilities for fetching source data
│       │   └── reading_data.py   <- Loads CSV into a pandas DataFrame
│       ├── data_exports/
│       │   └── write_csv.py      <- Saves DataFrames as named CSVs
│       └── utils/
│           ├── file_paths.py     <- Loads config.yaml
│           └── logging_config.py <- Configures file and console logging
│
├── tests/
│   ├── conftest.py               <- Shared pytest fixtures
│   └── unittests/
│
├── data_in/                      <- Place source CSV here (not committed)
├── data_out/                     <- Output CSVs written here (not committed)
└── logs/                         <- Log files written here (not committed)
```

---

## API Reference

The transformation functions used in this pipeline are provided by the [`cml_conversion_helpers`](https://pypi.org/project/cml-conversion-helpers/) package (pandas functions) and the [`cml_schemas`](https://pypi.org/project/cml-schemas/) package (output schema definitions). The key functions are documented below.

### Processing functions (`cml_conversion_helpers.pandas_functions.processing`)

All functions are available via `PROCESSING_FUNC_REGISTRY` for config-driven use.

---

#### `move_attributes_to_new_dimension`

Moves specified values from one column into a new dimension column. Rows whose `source_col_name` value is in `attributes_to_move` have that value placed into `new_col_name`, and `source_col_name` is replaced with `source_col_fill_value`. All other rows get `new_col_fill_value` in `new_col_name`.

```python
df = processing.move_attributes_to_new_dimension(
    df,
    source_col_name="Org_Code",
    source_col_fill_value="england",
    new_col_name="mbrrace_grouping",
    new_col_fill_value="no_mbrrace_grouping_filter",
    attributes_to_move=["Group 1. Level 3 NICU & NS", "Group 2. Level 3 NICU"]
)
```

---

#### `rename_cols`

Renames columns according to a mapping. Unmapped columns are left unchanged.

```python
df = processing.rename_cols(df, {"Org_Code": "location_id", "Final_value": "metric_value"})
```

---

#### `replace_col_values`

Replaces values in a column using a mapping dictionary.

```python
df = processing.replace_col_values(df, col_name="Org_Code", value_mappings={"ALL": "england"})
```

---

#### `concat_cols`

Concatenates multiple columns into a new column.

```python
df = processing.concat_cols(df, "metric_id", ["Dimension", "Count_Of"], prefix="", sep="_")
```

---

#### `create_uuid_col`

Adds a column containing a truncated UUID string (hyphens removed).

```python
df = processing.create_uuid_col(df, "datapoint_id", length=32)
```

---

#### `cast_date_col_to_timestamp`

Casts a string date column to a timestamp (default format: `dd/MM/yyyy`).

```python
df = processing.cast_date_col_to_timestamp(df, "reporting_period_start_datetime")
```

---

#### `add_lit_col`

Adds a new column populated with a constant value. Use `None` (Python) or `null` (YAML) for null.

```python
df = processing.add_lit_col(df, "publication_datetime", "01/12/2026")
df = processing.add_lit_col(df, "additional_metric_values", None)
```

---

#### `drop_cols`

Drops specified columns from a DataFrame.

```python
df = processing.drop_cols(df, ["unwanted_col_a", "unwanted_col_b"])
```

---

### Dimension functions (`cml_conversion_helpers.pandas_functions.dimension_cohorts`)

---

#### `create_dimension_columns`

Creates one column per dimension (populated with the attribute value for matching rows, `all_<dimension>` otherwise).

```python
df = dimension_cohorts.create_dimension_columns(
    df,
    "Dimension",
    "Measure",
    config["dimensions"],
    config["dimension_creation_exclusions"]
)
```

---

#### `create_dimension_type_col`

Adds a `dimension_type_id` column encoding the dimension type for each row.

```python
df = dimension_cohorts.create_dimension_type_col(df, config["dimensions"], "dimension_type_id")
```

---

#### `create_dimension_count_col`

Adds a `dimension_count` column indicating the number of active dimensions for each row.

```python
df = dimension_cohorts.create_dimension_count_col(df, config["dimensions"], "dimension_count")
```

---

#### `create_md5_hash_col`

Creates an MD5-based `dimension_id` from the dimension column values.

```python
df = dimension_cohorts.create_md5_hash_col(df, config["dimensions"], "dimension_id")
```

---

### Schema functions (`cml_schemas.pandas_schemas`)

---

#### `select_from_schema`

Selects columns from a DataFrame that match a given schema definition, producing the final output table.

```python
df_metric = pandas_schemas.select_from_schema(df, pandas_schemas.METRIC_SCHEMA)
```

---

### Extending the registry

You can add your own functions to `PROCESSING_FUNC_REGISTRY` using the `@register` decorator:

```python
from cml_conversion_helpers.pandas_functions.processing import register

@register
def my_custom_transform(df, some_param):
    # your logic here
    return df
```

---