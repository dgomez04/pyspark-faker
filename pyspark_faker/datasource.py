from pyspark.sql.datasource import DataSource
from pyspark.sql.types import StructType, StructField, StringType
from .reader import FakerReader

class FakerDataSource(DataSource):
    """
    A PySpark 4.0 custom data source that generates synthetic data using the Faker library.

    Options:
    --------
    - num_rows (int): number of rows to generate (default = 100)
    - seed (int): optional seed for reproducibility
    - locale (str): Faker locale, e.g., 'en_US', 'fr_FR'

    Example:
    --------
    >>> spark.read.format("faker") \
            .option("num_rows", 10) \
            .option("seed", 42) \
            .schema("name string, email string") \
            .load()
    """
    @classmethod
    def name(cls):
        return "faker"
    
    def schema(self):
        return StructType([
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("country", StringType(), True)
        ])
    
    def reader(self, schema) -> FakerReader:
        return FakerReader(schema, self.options)