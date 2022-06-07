import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark(scope='session'):
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("datalake") \
        .getOrCreate()
    return spark