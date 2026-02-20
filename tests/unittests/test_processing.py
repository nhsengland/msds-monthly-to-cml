import pytest
import pandas
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from src.processing import processing
from src.utils import spark as spark_utils

def test_move_attributes_to_new_dimension():
    """
    Tests move_attributes_to_new_dimension
    """
    spark = spark_utils.create_spark_session('tests')

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
    