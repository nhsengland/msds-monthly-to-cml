import re
import datetime

import pytest
import pandas
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from src.processing import processing
from src.utils import spark as spark_utils


def test_move_attributes_to_new_dimension(spark):
    """
    Tests move_attributes_to_new_dimension
    """

    test_data = [
        ('1',),
        ('2',),
        ('3',),
        ('a',),
        ('b',),
        ('c',),
    ]
    test_cols = ['existing_dim']
    df_test = spark.createDataFrame(test_data, test_cols)

    expected_data = [
        ('1','all_letters'),
        ('2','all_letters'),
        ('3','all_letters'),
        ('all_numbers','a'),
        ('all_numbers','b'),
        ('all_numbers','c'),
    ]
    expected_cols = ['existing_dim', 'new_dim']
    df_expected = spark.createDataFrame(expected_data, expected_cols)

    df_actual = processing.move_attributes_to_new_dimension(
        df_test,
        'existing_dim',
        'all_numbers',
        'new_dim',
        'all_letters',
        ['a', 'b', 'c']
    )

    assert df_actual.toPandas().equals(df_expected.toPandas())
    

def test_rename_cols(spark):
    """
    Tests rename_cols
    """

    test_data = [
        ("1", "2", "3", "4", "5"),
        ("1", "2", "3", "4", "5"),
        ("1", "2", "3", "4", "5"),
        ("1", "2", "3", "4", "5"),
    ]
    test_cols = ['1', "2", "3", "4", "5"]
    df_test = spark.createDataFrame(test_data, test_cols)

    expected_data = [
        ("1", "2", "3", "4", "5"),
        ("1", "2", "3", "4", "5"),
        ("1", "2", "3", "4", "5"),
        ("1", "2", "3", "4", "5"),
    ]
    expected_cols = ['1_new', "2", "3_new", "4", "5"]
    df_expected = spark.createDataFrame(expected_data, expected_cols)

    col_name_mappings = {
        "1": "1_new", # should be renamed
        "2": "2", # should be unchanged
        "3": "3_new",
        "6": "6_new", # not in df, should be ignored

    }

    df_actual = processing.rename_cols(
        df_test, col_name_mappings
    )

    assert df_actual.toPandas().equals(df_expected.toPandas()) 


def test_replace_col_values(spark):
    """
    Tests replace_col_values
    """

    test_data = [
        ("1", "2"),
        ("1", "2"),
        ("2", "2"),
        ("3", "2"),
    ]
    test_cols = ["col_1", "col_2"]
    df_test = spark.createDataFrame(test_data, test_cols)

    expected_data = [
        ("1_new", "2"),
        ("1_new", "2"),
        ("2_new", "2"),
        ("3", "2"),
    ]
    expected_cols = ["col_1", "col_2"]
    df_expected = spark.createDataFrame(expected_data, expected_cols)

    value_mappings = {
        "1": "1_new",
        "2": "2_new",
    }

    df_actual = processing.replace_col_values(
        df_test, value_mappings, "col_1"
    )

    assert df_actual.toPandas().equals(df_expected.toPandas()) 


@pytest.mark.parametrize("test_data, mappings, col_name, expected_data", [
    ([("1", "A")], {"1": "1_new"}, "col_1", [("1_new", "A")]), # basic replacement - string
    ([(10, "E")], {10: 100}, "col_1", [(100, "E")]), # basic replacement - integer
    ([("3", "C"), ("4", "C")], {"3": None}, "col_1", [(None, "C"), ("4", "C")]), # replace with null, and ignore value not in mapping
    ([("2", "B")], {"1": "1_new"}, "col_1", [("2", "B")]), # values not in col - should not change
    ([("1", "D")], {}, "col_1", [("1", "D")]), # Empty mapping - should not change
])
def test_replace_col_values_parametrized(spark, test_data, mappings, col_name, expected_data):
    
    col_names = ["col_1", "col_2"]
    df_test = spark.createDataFrame(test_data, col_names)
    df_expected = spark.createDataFrame(expected_data, col_names)
    df_actual = processing.replace_col_values(df_test, mappings, col_name)

    assert df_actual.toPandas().equals(df_expected.toPandas()) 


def test_concat_cols(spark):
    """
    Tests concat_cols
    """

    test_data = [
        ("1", "2", "3", "4", "5"), # basic concatenation
        ("1", " ", "3", "4", "5"), # whitespace
        ("1", "2", None, "4", "5"), # null value
    ]
    test_cols = ["1", "2", "3", "4", "5"]
    df_test = spark.createDataFrame(test_data, test_cols)

    expected_data = [
        ("1", "2", "3", "4", "5", "1|2|3|4|5"),
        ("1", " ", "3", "4", "5", "1| |3|4|5"),
        ("1", "2", None, "4", "5", "1|2|4|5"),
    ]
    expected_cols = ["1", "2", "3", "4", "5", "6"]
    df_expected = spark.createDataFrame(expected_data, expected_cols)
    
    cols_to_concat = ["1", "2", "3", "4", "5"]

    df_actual = processing.concat_cols(
        df_test, "6", cols_to_concat, "", "|"
    )

    assert df_actual.toPandas().equals(df_expected.toPandas()) 


def test_concat_cols_with_prefix(spark):
    """
    Tests concat_cols
    """

    test_data = [
        ("1", "2", "3", "4", "5"), # basic concatenation
        ("1", " ", "3", "4", "5"), # whitespace
        ("1", "2", None, "4", "5"), # null value
    ]
    test_cols = ["all_1", "all_2", "all_3", "all_4", "all_5"]
    df_test = spark.createDataFrame(test_data, test_cols)

    expected_data = [
        ("1", "2", "3", "4", "5", "1|2|3|4|5"),
        ("1", " ", "3", "4", "5", "1| |3|4|5"),
        ("1", "2", None, "4", "5", "1|2|4|5"),
    ]
    expected_cols = ["all_1", "all_2", "all_3", "all_4", "all_5", "6"]
    df_expected = spark.createDataFrame(expected_data, expected_cols)
    
    cols_to_concat = ["1", "2", "3", "4", "5"]

    df_actual = processing.concat_cols(
        df_test, "6", cols_to_concat, "all_", "|"
    )

    assert df_actual.toPandas().equals(df_expected.toPandas()) 


def test_create_uuid_col(spark):
    """
    Tests create_uuid_col.
    Since UUIDs are random, we assert structural properties rather than exact values:
    - the new column is present
    - every value has exactly the requested length
    - values consist only of valid hex characters (hyphens stripped before truncation)
    - values are unique across rows
    """


    test_data = [("a",), ("b",), ("c",), ("d",), ("e",)]
    df_test = spark.createDataFrame(test_data, ["existing_col"])

    uuid_length = 12
    df_actual = processing.create_uuid_col(df_test, "row_id", uuid_length)

    assert "row_id" in df_actual.columns

    ids = [row["row_id"] for row in df_actual.collect()]

    assert all(len(id_) == uuid_length for id_ in ids), "All IDs should have the requested length"
    assert all(re.fullmatch(r"[0-9a-f]+", id_) for id_ in ids), "All IDs should be lowercase hex"
    assert len(set(ids)) == len(ids), "All IDs should be unique"


def test_cast_date_col_to_timestamp(spark):
    """
    Tests cast_date_col_to_timestamp converts a DateType column to timestamp at midnight.
    Uses default format dd/MM/yyyy.
    """

    schema = StructType([StructField("event_date", StringType())])
    test_data = [
        ("15/01/2024",),
        ("01/06/2000",),
        (None,),
    ]
    df_test = spark.createDataFrame(test_data, schema)

    df_actual = processing.cast_date_col_to_timestamp(df_test, "event_date")

    assert df_actual.schema["event_date"].dataType == TimestampType()

    rows = df_actual.collect()
    assert rows[0]["event_date"] == datetime.datetime(2024, 1, 15, 0, 0, 0)
    assert rows[1]["event_date"] == datetime.datetime(2000, 6, 1, 0, 0, 0)
    assert rows[2]["event_date"] is None


def test_cast_date_col_to_timestamp_custom_format(spark):
    """
    Tests cast_date_col_to_timestamp with a non-default format string.
    """

    schema = StructType([StructField("event_date", StringType())])
    test_data = [("2024-01-15",), ("2000-06-01",)]
    df_test = spark.createDataFrame(test_data, schema)

    df_actual = processing.cast_date_col_to_timestamp(df_test, "event_date", format="yyyy-MM-dd")

    rows = df_actual.collect()
    assert rows[0]["event_date"] == datetime.datetime(2024, 1, 15, 0, 0, 0)
    assert rows[1]["event_date"] == datetime.datetime(2000, 6, 1, 0, 0, 0)


def test_drop_cols(spark):
    """
    Tests drop_cols removes the specified columns and leaves others intact.
    """
    test_data = [("a", "b", "c")]
    df_test = spark.createDataFrame(test_data, ["col_1", "col_2", "col_3"])

    df_actual = processing.drop_cols(df_test, ["col_1", "col_3"])

    assert df_actual.columns == ["col_2"]
    assert df_actual.count() == 1


def test_drop_cols_nonexistent(spark):
    """
    Tests drop_cols silently ignores columns that don't exist in the dataframe.
    """
    test_data = [("a", "b")]
    df_test = spark.createDataFrame(test_data, ["col_1", "col_2"])

    df_actual = processing.drop_cols(df_test, ["col_1", "col_99"])

    assert df_actual.columns == ["col_2"]


def test_add_lit_col(spark):
    """
    Tests add_lit_col adds a new column with the given literal value on every row.
    """
    test_data = [("a",), ("b",), ("c",)]
    df_test = spark.createDataFrame(test_data, ["existing_col"])

    df_actual = processing.add_lit_col(df_test, "publication_date", "01/01/2001")

    assert "publication_date" in df_actual.columns
    values = [row["publication_date"] for row in df_actual.collect()]
    assert values == ["01/01/2001", "01/01/2001", "01/01/2001"]


def test_add_lit_col_does_not_affect_other_columns(spark):
    """
    Tests add_lit_col leaves existing columns untouched.
    """
    test_data = [("x", "y")]
    df_test = spark.createDataFrame(test_data, ["col_1", "col_2"])

    df_actual = processing.add_lit_col(df_test, "new_col", "val")

    row = df_actual.collect()[0]
    assert row["col_1"] == "x"
    assert row["col_2"] == "y"
