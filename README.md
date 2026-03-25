# Maternity Services Statistics to CML Schema Conversion

## Overview

This pipeline converts the **Maternity Services Monthly Statistics** into the format required by the **NHS England Central Metrics Library (CML)**.

The source data is the Official Statistics about NHS-funded maternity services in England, drawn from the Maternity Services Data Set (MSDS). It covers activity at the booking appointment, during pregnancy, during and after birth, and information on pregnancy outcomes.

The pipeline takes the MSDS data in a tidy (long) format, applies a series of configurable transformations, and produces two output tables matching the CML schema: a **metric table** and a **dimensions table**.

---

## Prerequisites

- Python >= 3.10
- Java 8 or 11 (required by PySpark — ensure `JAVA_HOME` is set)
- [Poetry](https://python-poetry.org/) for dependency management

---

## Getting Started

### Installation

If you load in Codespaces, the relevant packages should install automatically - it may take a few minutes!

```bash
poetry install
```

### Running the pipeline

Create the virtual environment and run the script:

```bash
eval $(poetry env activate)
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

Two CSVs are written to `data_out/`:

### Metric table (`data_out/metric/metric.csv`)

One row per data point, containing the numeric value and metadata:

| datapoint_id | metric_id | metric_dimension_id | location_id | location_type | metric_value | reporting_period_start_datetime | last_record_timestamp | publication_date | last_ingest_timestamp | additional_metric_values |
|---|---|---|---|---|---|---|---|---|---|---|

### Dimensions table (`data_out/dimensions/dimensions.csv`)

One row per data point, one column per dimension. Each dimension column defaults to `all_<dimension>` unless the data point belongs to that dimension:

| datapoint_id | metric_dimension_id | dimension_cohort_id | EthnicCategoryMotherGroup | AgeAtBookingMotherGroup | ... |
|---|---|---|---|---|---|

The `dimension_cohort_id` is a `|`-separated concatenation of all dimension column values and links the metric and dimensions tables together.

---

## Pipeline Steps

The transformation logic is defined as an ordered sequence in `config.yaml` under `processing_funcs`. Each entry maps to a registered function in the `cml_conversion_helpers` library via `PROCESSING_FUNC_REGISTRY`.

The steps applied in this pipeline are:

1. **`move_attributes_to_new_dimension`** — moves MBRRACE grouping values (e.g. `"Group 1. Level 3 NICU & NS"`) out of `Org_Code` into a new `mbrrace_grouping` column
2. **`replace_col_values`** — replaces `"ALL"` in `Org_Code` with `"england"`
3. **`rename_cols`** — renames source columns to CML schema names (`Org_Code` → `location_id`, etc.)
4. **`cast_date_col_to_timestamp`** — casts date string columns to timestamps
5. **`create_uuid_col`** — generates a unique `datapoint_id` per row
6. **`concat_cols`** — builds `metric_id` by concatenating `Dimension` and `Count_Of`
7. **`add_lit_col`** + **`cast_date_col_to_timestamp`** — adds `publication_date` and `last_ingest_timestamp` as typed columns
8. **`add_lit_col`** — adds `additional_metric_values` as a null column

You don't have to use this config-driven approach if you don't want to. You can simply add your PySpark code into the create_cml_tables.py file.

After these steps, `create_dimension_table` builds the per-dimension columns and `dimension_cohort_id`, and a final `concat_cols` call builds `metric_dimension_id`.

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
│       │   └── reading_data.py   <- Loads CSV into a Spark DataFrame
│       ├── data_exports/
│       │   └── write_csv.py      <- Saves Spark DataFrames as named CSVs
│       └── utils/
│           ├── file_paths.py     <- Loads config.yaml
│           ├── logging_config.py <- Configures file and console logging
│           └── spark.py          <- Creates and configures a SparkSession
│
├── tests/
│   ├── conftest.py               <- Shared pytest fixtures (SparkSession)
│   └── unittests/
│       └── test_spark.py
│
├── data_in/                      <- Place source CSV here (not committed)
├── data_out/                     <- Output CSVs written here (not committed)
└── logs/                         <- Log files written here (not committed)
```

---

## API Reference

The transformation functions used in this pipeline are provided by the [`cml_conversion_helpers`](https://pypi.org/project/cml-conversion-helpers/) package. The key functions are documented below.

### Processing functions (`cml_conversion_helpers.processing.processing`)

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
df = processing.replace_col_values(df, {"ALL": "england"}, "Org_Code")
```

---

#### `concat_cols`

Concatenates multiple columns into a new column.

```python
df = processing.concat_cols(df, "metric_id", ["Dimension", "Count_Of"], sep="_")
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
df = processing.add_lit_col(df, "publication_date", "01/12/2026")
df = processing.add_lit_col(df, "additional_metric_values", None)
```

---

#### `drop_cols`

Drops specified columns from a DataFrame.

```python
df = processing.drop_cols(df, ["unwanted_col_a", "unwanted_col_b"])
```

---

### Dimension functions (`cml_conversion_helpers.processing.dimension_cohorts`)

---

#### `create_dimension_table`

Main entry point for building the dimensions table. Creates one column per dimension (populated with the attribute value for matching rows, `all_<dimension>` otherwise) and a `dimension_cohort_id` column.

```python
df = dimension_cohorts.create_dimension_table(
    df,
    dimension_cols=config["dimensions"],
    dimensions_to_exclude=config["dimension_creation_exclusions"]
)
```

---

#### `get_dimension_list_from_col`

Extracts the list of distinct values from a dimension column — useful when you want to derive the dimension list from the data rather than hard-coding it in config.

```python
dimensions = dimension_cohorts.get_dimension_list_from_col(df, "Dimension")
```

---

### Extending the registry

You can add your own functions to `PROCESSING_FUNC_REGISTRY` using the `@register` decorator:

```python
from cml_conversion_helpers.processing.processing import register

@register
def my_custom_transform(df, some_param):
    # your logic here
    return df
```

---