from pyspark.sql.datasource import DataSourceReader
from pyspark.sql.types import StructType
from typing import List, Optional

"""
A simple batch reader implementation for exploring PySpark 4.0's custom data source API.

It starts with a base schema and ingests a default number of synthetic records. 
Both the schema and row count are configurable through user-defined options, allowing for flexible testing and prototyping.
"""


class FakerReader(DataSourceReader):
    def __init__(self, schema, options) -> None:
        self.schema: StructType = schema
        self.options = options
        self.total_rows = int(options.get("num_rows", 100))
        self.seed = int(options.get("seed")) if options.get("seed") is not None else None
        self.locale = options.get("locale")
        self.num_partitions = int(options.get("num_partitions", 4))  # Default to 4 partitions

    def read(self, partition: Optional[dict] = None) -> List[tuple]:
        from .faker_engine import FakeRowGenerator
        
        # Calculate rows per partition
        partition_id = partition.get("partition_id", 0) if partition else 0
        rows_per_partition = self.total_rows // self.num_partitions
        remaining_rows = self.total_rows % self.num_partitions
        
        # Adjust rows for this partition
        if partition_id < remaining_rows:
            rows_per_partition += 1
            
        # Calculate start and end indices for this partition
        start_idx = partition_id * rows_per_partition
        if partition_id < remaining_rows:
            start_idx += partition_id
        else:
            start_idx += remaining_rows
            
        # Create generator with partition-specific seed
        partition_seed = self.seed + partition_id if self.seed is not None else None
        generator = FakeRowGenerator(
            schema=self.schema,
            seed=partition_seed,
            locale=self.locale
        )
        
        return list(generator.generate_rows(rows_per_partition))

    def partitions(self) -> List[dict]:
        """Return a list of partition information."""
        return [{"partition_id": i} for i in range(self.num_partitions)]



