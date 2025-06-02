from pyspark.sql.datasource import DataSourceReader
from pyspark.sql.types import StructType

"""
A simple batch reader implementation for exploring PySpark 4.0's custom data source API.

It starts with a base schema and ingests a default number of synthetic records. 
Both the schema and row count are configurable through user-defined options, allowing for flexible testing and prototyping.
"""


class FakerReader(DataSourceReader):
    def __init__(self, schema, options) -> None:
        self.schema : StructType = schema
        self.options = options
        self.num_rows = int(options.get("num_rows", 100))
        self.seed = int(options.get("seed")) if options.get("seed") is not None else None
        self.locale = options.get("locale")

        from .faker_engine import FakeRowGenerator
        self.generator = FakeRowGenerator(
            schema=self.schema,
            seed=self.seed,
            locale=self.locale
        )

    def read(self, partition=None):
        return self.generator.generate_rows(self.num_rows)



