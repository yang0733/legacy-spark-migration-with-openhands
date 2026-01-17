"""
Modern ETL Job - Databricks Notebook Version
Optimized for Databricks notebook environment where 'spark' is pre-available

This version assumes the SparkSession is already created and available as 'spark'
which is the standard in Databricks notebooks.
"""

from pyspark.sql.functions import (
    col, upper, trim, when, to_timestamp, 
    unix_timestamp, coalesce, lit
)
from pyspark.sql.types import StructType, StructField, StringType, LongType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def define_schema():
    """
    Define explicit schema for better performance and type safety
    """
    return StructType([
        StructField("timestamp_str", StringType(), True),
        StructField("user_id", StringType(), True)
    ])


def clean_and_transform_data(df):
    """
    Modern data transformation using built-in Spark functions
    Replaces slow Python UDF with vectorized operations
    """
    logger.info("Starting data transformation...")
    
    # IMPROVEMENT 1: Replace Python UDF with built-in functions
    # This is much faster as it stays in the JVM without serialization overhead
    df_cleaned = df.withColumn(
        "clean_user",
        when(col("user_id").isNotNull(), 
             upper(trim(col("user_id"))))
        .otherwise(lit(None))
    )
    
    # IMPROVEMENT 2: Modern datetime parsing compatible with Spark 3.5
    # Using to_timestamp which handles the Proleptic Gregorian calendar correctly
    df_with_timestamp = df_cleaned.withColumn(
        "parsed_timestamp",
        to_timestamp(col("timestamp_str"), "yyyy-MM-dd hh:mm:ss a")
    ).withColumn(
        "unix_ts",
        unix_timestamp(col("parsed_timestamp"))
    )
    
    # IMPROVEMENT 3: Add data quality checks
    df_final = df_with_timestamp.withColumn(
        "is_valid_timestamp",
        col("parsed_timestamp").isNotNull()
    ).withColumn(
        "is_valid_user",
        col("clean_user").isNotNull()
    )
    
    logger.info("Data transformation completed")
    return df_final


def run_etl():
    """
    Main ETL pipeline - uses existing 'spark' session from Databricks notebook
    """
    try:
        logger.info("Starting Modern ETL Job (Notebook Version)...")
        
        # Create sample data with explicit schema
        schema = define_schema()
        data = [("2023-01-01 10:00:00 AM", "user_1"), (None, "user_2")]
        
        # Use explicit schema for better performance
        df = spark.createDataFrame(data, schema)
        
        # Transform the data using modern patterns
        df_transformed = clean_and_transform_data(df)
        
        # Better output with column selection and ordering
        result_df = df_transformed.select(
            "user_id",
            "clean_user", 
            "timestamp_str",
            "parsed_timestamp",
            "unix_ts",
            "is_valid_timestamp",
            "is_valid_user"
        ).orderBy("user_id")
        
        # Show results with better formatting
        logger.info("Displaying transformation results:")
        result_df.show(truncate=False)
        
        # Add data quality summary
        total_records = result_df.count()
        valid_timestamps = result_df.filter(col("is_valid_timestamp")).count()
        valid_users = result_df.filter(col("is_valid_user")).count()
        
        logger.info(f"Data Quality Summary:")
        logger.info(f"Total records: {total_records}")
        logger.info(f"Valid timestamps: {valid_timestamps}/{total_records}")
        logger.info(f"Valid users: {valid_users}/{total_records}")
        
        logger.info("Modern ETL Job completed successfully")
        return result_df
        
    except Exception as e:
        logger.error(f"ETL Job failed: {str(e)}")
        raise


# For notebook execution, just call the function
# No session management needed since 'spark' is already available
if __name__ == "__main__":
    # This will work in standalone Python execution
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("ModernETLJob").getOrCreate()
    try:
        run_etl()
    finally:
        spark.stop()
else:
    # In Databricks notebook, just run the ETL
    # Uncomment the next line to run automatically when imported
    # result_df = run_etl()
    pass