from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, unix_timestamp, from_unixtime
from pyspark.sql.types import StringType

# 1. LEGACY ENTRY POINT (Deprecated in Spark 3.0+)
# Use SparkSession instead of SQLContext for serverless compatibility
spark = SparkSession.builder.getOrCreate()

# Create dummy data
data = [("2023-01-01 10:00:00 AM", "user_1"), (None, "user_2")]
df = spark.createDataFrame(data, ["timestamp_str", "user_id"])

# 2. PERFORMANCE KILLER: Row-by-row Python UDF
# This is slow because it serializes data between JVM and Python for every row
def legacy_upper_clean(s):
    if s:
        return s.strip().upper()
    return None

# Standard UDF is untyped/slow compared to Spark 3 Vectorized Pandas UDFs
upper_udf = udf(legacy_upper_clean, StringType())

# 3. BREAKING CHANGE: Legacy DateTime Parsing
# Spark 3 uses the Proleptic Gregorian calendar; this pattern often fails 
# without setting spark.sql.legacy.timeParserPolicy=LEGACY
df_transformed = df.withColumn(
    "clean_user", upper_udf(df["user_id"])
).withColumn(
    "unix_ts", unix_timestamp(df["timestamp_str"], 'yyyy-MM-dd hh:mm:ss aa')
)

# 4. DEPRECATED ACTION
# Show the results using the old SQLContext pattern
df_transformed.show()