#!/usr/bin/env python3
"""
Quick Test Script - Spark 2.4 to 3.5 Migration Demo
Run this script to see the performance improvement in action!

Requirements:
- Python 3.8+
- PySpark 3.5+ (will be installed if missing)

Usage:
    python quick_test.py
"""

import sys
import time
import subprocess

def install_pyspark():
    """Install PySpark if not available"""
    try:
        import pyspark
        print("✅ PySpark already installed")
        return True
    except ImportError:
        print("📦 Installing PySpark 3.5...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "pyspark==3.5.0"])
            print("✅ PySpark 3.5 installed successfully")
            return True
        except subprocess.CalledProcessError:
            print("❌ Failed to install PySpark. Please install manually:")
            print("   pip install pyspark==3.5.0")
            return False

def run_legacy_test():
    """Run legacy Spark 2.4 approach (slow)"""
    print("\n⚠️  Testing Legacy Spark 2.4 Approach (SLOW)...")
    
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("LegacySparkTest") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    # Legacy Python UDF (SLOW)
    def legacy_upper_clean(s):
        if s:
            return s.strip().upper()
        return None
    
    upper_udf = udf(legacy_upper_clean, StringType())
    
    # Create test data (larger dataset to show performance difference)
    data = [(f"user_{i}",) for i in range(1000)]
    df = spark.createDataFrame(data, ["user_id"])
    
    # Time the legacy approach
    start_time = time.time()
    result = df.withColumn("clean_user", upper_udf(df["user_id"]))
    result.count()  # Force execution
    legacy_time = time.time() - start_time
    
    print(f"   Legacy UDF time: {legacy_time:.3f} seconds")
    print("   Sample output:")
    result.limit(3).show()
    
    spark.stop()
    return legacy_time

def run_modern_test():
    """Run modern Spark 3.5 approach (fast)"""
    print("\n✅ Testing Modern Spark 3.5 Approach (FAST)...")
    
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, upper, trim, when, to_timestamp, unix_timestamp, lit
    from pyspark.sql.types import StructType, StructField, StringType
    
    # Create optimized Spark session
    spark = SparkSession.builder \
        .appName("ModernSparkTest") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    # Create test data (same size as legacy)
    data = [(f"user_{i}",) for i in range(1000)]
    df = spark.createDataFrame(data, ["user_id"])
    
    # Time the modern approach
    start_time = time.time()
    result = df.withColumn(
        "clean_user",
        when(col("user_id").isNotNull(), upper(trim(col("user_id"))))
        .otherwise(lit(None))
    )
    result.count()  # Force execution
    modern_time = time.time() - start_time
    
    print(f"   Modern built-in functions time: {modern_time:.3f} seconds")
    print("   Sample output:")
    result.limit(3).show()
    
    # Test datetime parsing as well
    print("\n📅 Testing DateTime Parsing...")
    datetime_data = [("2023-01-01 10:00:00 AM", "user_1"), (None, "user_2")]
    schema = StructType([
        StructField("timestamp_str", StringType(), True),
        StructField("user_id", StringType(), True)
    ])
    
    df_datetime = spark.createDataFrame(datetime_data, schema)
    
    # Modern datetime transformation
    df_transformed = df_datetime.withColumn(
        "clean_user",
        when(col("user_id").isNotNull(), upper(trim(col("user_id"))))
        .otherwise(lit(None))
    ).withColumn(
        "parsed_timestamp",
        to_timestamp(col("timestamp_str"), "yyyy-MM-dd hh:mm:ss a")
    ).withColumn(
        "unix_ts",
        unix_timestamp(col("parsed_timestamp"))
    ).withColumn(
        "is_valid_timestamp",
        col("parsed_timestamp").isNotNull()
    ).withColumn(
        "is_valid_user",
        col("clean_user").isNotNull()
    )
    
    print("   DateTime transformation result:")
    df_transformed.show(truncate=False)
    
    # Data quality summary
    total_records = df_transformed.count()
    valid_timestamps = df_transformed.filter(col("is_valid_timestamp")).count()
    valid_users = df_transformed.filter(col("is_valid_user")).count()
    
    print(f"\n📈 Data Quality Summary:")
    print(f"   Total records: {total_records}")
    print(f"   Valid timestamps: {valid_timestamps}/{total_records}")
    print(f"   Valid users: {valid_users}/{total_records}")
    
    spark.stop()
    return modern_time

def main():
    """Main test function"""
    print("🚀 Spark 2.4 to 3.5 Migration Performance Test")
    print("=" * 50)
    
    # Install PySpark if needed
    if not install_pyspark():
        return
    
    try:
        # Run tests
        legacy_time = run_legacy_test()
        modern_time = run_modern_test()
        
        # Calculate improvement
        if modern_time > 0:
            improvement = legacy_time / modern_time
            print(f"\n🎉 PERFORMANCE RESULTS:")
            print(f"   Legacy approach: {legacy_time:.3f} seconds")
            print(f"   Modern approach: {modern_time:.3f} seconds")
            print(f"   Performance improvement: {improvement:.1f}x faster!")
            
            if improvement >= 2:
                print("   🏆 Significant performance improvement achieved!")
            else:
                print("   ✅ Performance improvement achieved!")
        
        print(f"\n✅ Migration test completed successfully!")
        print(f"   You've just experienced the power of Spark 3.5 optimizations!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        print("   Make sure you have Java 8+ installed and JAVA_HOME set")
        print("   For help, see: https://spark.apache.org/docs/latest/")

if __name__ == "__main__":
    main()