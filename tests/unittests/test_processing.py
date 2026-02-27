import pytest
import pandas
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

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
        ("1", "2"),
        ("1", "2"),
    ]
    test_cols = ["col_1", "col_2"]
    df_test = spark.createDataFrame(test_data, test_cols)

    expected_data = [
        ("1_new", "2"),
        ("1_new", "2"),
        ("1_new", "2"),
        ("1_new", "2"),
    ]
    expected_cols = ["col_1", "col_2"]
    df_expected = spark.createDataFrame(expected_data, expected_cols)

    value_mappings = {
        "1": "1_new",
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