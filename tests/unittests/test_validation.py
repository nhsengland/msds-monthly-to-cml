import pytest
import datetime
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, IntegerType
)

from src.validation import validation


SCHEMA = StructType([
    StructField("id",         StringType(),    nullable=False),
    StructField("created_at", TimestampType(), nullable=False),
    StructField("label",      StringType(),    nullable=True),
])


# --- select_from_schema ---

def test_select_from_schema_returns_correct_columns(spark):
    test_data = [("1", datetime.datetime(2024, 1, 1), "a", "extra")]
    df = spark.createDataFrame(test_data, ["id", "created_at", "label", "unwanted"])

    result = validation.select_from_schema(df, SCHEMA)

    assert result.columns == ["id", "created_at", "label"]
    assert "unwanted" not in result.columns


def test_select_from_schema_preserves_column_order(spark):
    test_data = [("a", datetime.datetime(2024, 1, 1), "x")]
    # Create df with columns in a different order
    df = spark.createDataFrame(test_data, ["label", "created_at", "id"])

    result = validation.select_from_schema(df, SCHEMA)

    assert result.columns == ["id", "created_at", "label"]


# --- validate_schema ---

def test_validate_schema_passes_for_matching_schema(spark):
    test_data = [("1", datetime.datetime(2024, 1, 1), "a")]
    df = spark.createDataFrame(test_data, ["id", "created_at", "label"])

    validation.validate_schema(df, SCHEMA)  # should not raise


def test_validate_schema_raises_on_wrong_type(spark):
    wrong_schema = StructType([
        StructField("id",         StringType()),
        StructField("created_at", StringType()),  # should be timestamp
        StructField("label",      StringType()),
    ])
    test_data = [("1", "2024-01-01", "a")]
    df = spark.createDataFrame(test_data, wrong_schema)

    with pytest.raises(TypeError, match="created_at"):
        validation.validate_schema(df, SCHEMA)


def test_validate_schema_raises_on_missing_column(spark):
    test_data = [("1", datetime.datetime(2024, 1, 1))]
    df = spark.createDataFrame(test_data, ["id", "created_at"])  # label missing

    with pytest.raises(TypeError, match="label"):
        validation.validate_schema(df, SCHEMA)


def test_validate_schema_reports_all_errors_at_once(spark):
    wrong_schema = StructType([
        StructField("id", IntegerType()),  # wrong type
        # created_at missing entirely
        StructField("label", StringType()),
    ])
    test_data = [(1, "a")]
    df = spark.createDataFrame(test_data, wrong_schema)

    with pytest.raises(TypeError) as exc_info:
        validation.validate_schema(df, SCHEMA)

    error_message = str(exc_info.value)
    assert "id" in error_message
    assert "created_at" in error_message
