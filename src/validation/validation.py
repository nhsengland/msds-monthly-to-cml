from pyspark.sql import DataFrame


def select_from_schema(df: DataFrame, schema) -> DataFrame:
    return df.select(*[field.name for field in schema.fields])


def validate_schema(df: DataFrame, schema) -> None:
    df_types = dict(df.dtypes)
    errors = []

    for field in schema.fields:
        col_name = field.name

        if col_name not in df_types:
            errors.append(f"  - '{col_name}': column is missing")
            continue

        actual = df_types[col_name]
        expected = field.dataType.simpleString()
        if actual != expected:
            errors.append(f"  - '{col_name}': expected {expected}, got {actual}")

    if errors:
        raise TypeError("Schema validation failed:\n" + "\n".join(errors))
