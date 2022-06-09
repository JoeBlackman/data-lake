import os
import pathlib
import pytest
from pyspark.sql import SparkSession
import shutil


test_dir = pathlib.Path(__file__).parent.resolve()


@pytest.fixture(scope='session')
def spark(scope='session'):
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("datalake") \
        .getOrCreate()
    return spark


@pytest.fixture(scope='session')
def clear_data_lake(scope='session'):
    data_lake_dir = f'{test_dir}/data/data_lake'
    for table in os.listdir(data_lake_dir):
        shutil.rmtree(f'{data_lake_dir}/{table}')
