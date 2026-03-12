import pytest
import pandas
import hashlib

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from src.processing import dimension_cohorts
from src.utils import spark as spark_utils
    

def test_get_dimension_list_from_col(spark):
    """
    Tests get_dimension_list_from_col
    """

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


def test_create_md5_hash_col(spark):
    """
    Tests create_md5_hash_col produces a consistent MD5 hash from multiple columns.
    """
    

    test_data = [
        ('alice', '42'),
        ('bob', '99'),
        ('alice', '42'),
    ]
    test_cols = ['name', 'age']
    df_test = spark.createDataFrame(test_data, test_cols)

    result = dimension_cohorts.create_md5_hash_col(df_test, ['name', 'age'], 'row_hash')

    assert 'row_hash' in result.columns

    rows = result.orderBy('name', 'age').collect()

    # Verify the hash matches what Python's hashlib produces for the same concat_ws logic
    def expected_md5(name, age):
        return hashlib.md5(f"{name}|{age}".encode()).hexdigest()

    assert rows[0]['row_hash'] == expected_md5('alice', '42')
    assert rows[1]['row_hash'] == expected_md5('alice', '42')
    assert rows[2]['row_hash'] == expected_md5('bob', '99')

    # Same inputs should produce the same hash
    assert rows[0]['row_hash'] == rows[1]['row_hash']
