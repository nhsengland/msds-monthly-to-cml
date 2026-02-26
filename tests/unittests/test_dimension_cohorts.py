import pytest
import pandas
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from src.processing import dimension_cohorts
from src.utils import spark as spark_utils
    

def test_get_dimension_list_from_col():
    """
    Tests get_dimension_list_from_col
    """
    spark = spark_utils.create_spark_session('tests')

    test_data = [
        ('1',),
        ('2',),
        ('2',),
        ('A',),
        ('A',),
        ('A',),
        ('A',),
    ]
    test_cols = ['existing_dim']
    df_test = spark.createDataFrame(test_data, test_cols)

    expected = ['1', '2', 'A']

    actual = dimension_cohorts.get_dimension_list_from_col(
        df_test,
        'existing_dim',
    )

    assert expected == actual
