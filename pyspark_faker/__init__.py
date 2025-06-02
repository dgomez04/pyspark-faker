from .datasource import FakerDataSource
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
if spark:
    spark.dataSource.register(FakerDataSource)