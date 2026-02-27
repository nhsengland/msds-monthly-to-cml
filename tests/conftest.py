import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .master("local[1]")
               .appName("pytest-pyspark-local")
               .config("spark.sql.shuffle.partitions", "1")
               .getOrCreate())
    yield session
    session.stop()
