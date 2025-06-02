from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, DateType, TimestampType,
    ArrayType, MapType
)

# Create a Spark session with more executors
spark = (SparkSession.builder
    .appName("FakerTest")
    .master("local[4]")  # Use 4 local cores
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate())

# Register the Faker data source
from pyspark_faker import FakerDataSource
spark.dataSource.register(FakerDataSource)

# Define a comprehensive test schema
schema = StructType([
    # Personal Information
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("ssn", StringType(), True),
    
    # Address Information
    StructField("street_address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zipcode", StringType(), True),
    StructField("country", StringType(), True),
    
    # Company Information
    StructField("company", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("department", StringType(), True),
    
    # Internet Related
    StructField("username", StringType(), True),
    StructField("password", StringType(), True),
    StructField("url", StringType(), True),
    StructField("ipv4", StringType(), True),
    
    # Dates and Times
    StructField("birth_date", DateType(), True),
    StructField("hire_date", DateType(), True),
    StructField("last_login", TimestampType(), True),
    
    # Numeric Fields
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True),
    StructField("years_experience", IntegerType(), True),
    
    # Boolean Fields
    StructField("is_active", BooleanType(), True),
    StructField("is_verified", BooleanType(), True),
    
    # Credit Card Information
    StructField("credit_card_number", StringType(), True),
    StructField("credit_card_expire", StringType(), True),
    StructField("credit_card_provider", StringType(), True)
])

# Create a DataFrame using the Faker data source with partitioning
df = (spark.read.format("faker")
    .option("num_rows", 1000)  # Generate more rows to see distribution
    .option("num_partitions", 4)  # Use 4 partitions
    .option("seed", 42)
    .schema(schema)
    .load())

# Show the data
print("\nGenerated Data (first 10 rows):")
df.show(10, truncate=False)

# Show the schema
print("\nSchema:")
df.printSchema()

# Show partition information
print("\nPartition Information:")
print(f"Number of partitions: {df.rdd.getNumPartitions()}")
print("\nPartition sizes:")
df.rdd.glom().map(len).collect()

# Show some basic statistics
print("\nBasic Statistics:")
df.describe("age", "salary", "years_experience").show() 