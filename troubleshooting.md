# Troubleshooting Guide: Common Spark Migration Issues

## 🚨 **Common Errors and Solutions**

This guide helps you troubleshoot common issues when migrating from Spark 2.4 to 3.5. Each error includes the exact error message, root cause, and solution.

---

## ❌ **Error 1: Serverless Compute Issues**

### **Error Messages:**
```
ERROR: PERSIST TABLE not supported in serverless compute
ERROR: Cache operations are not available in serverless environments
ERROR: Cannot use .cache() in serverless mode
```

### **Root Cause:**
Your code contains caching operations that aren't supported in Databricks serverless compute.

### **Solution:**
Remove or replace caching operations:

```python
# ❌ DON'T DO THIS (not supported in serverless)
df.cache()
df.persist()

# ✅ DO THIS INSTEAD (serverless compatible)
# Let serverless compute handle optimization automatically
# No explicit caching needed
```

### **Prevention:**
- Use serverless compute's built-in optimizations
- Avoid manual caching in serverless environments
- Let Adaptive Query Execution handle optimization

---

## ❌ **Error 2: Session Already Exists**

### **Error Messages:**
```
ERROR: SESSION_ALREADY_EXIST
Cannot create a new SparkSession when one already exists
IllegalStateException: Cannot call getOrCreate() when session already exists
```

### **Root Cause:**
Databricks notebooks have pre-existing Spark sessions, but your code tries to create a new one.

### **Solution:**
Use smart session detection:

```python
# ❌ DON'T DO THIS (causes conflicts in notebooks)
spark = SparkSession.builder.getOrCreate()

# ✅ DO THIS INSTEAD (works everywhere)
try:
    # Use existing session in notebooks
    spark = SparkSession.getActiveSession()
    if spark is None:
        # Create new session in standalone environments
        spark = SparkSession.builder.appName("MyApp").getOrCreate()
except:
    spark = SparkSession.builder.appName("MyApp").getOrCreate()
```

### **Prevention:**
- Use the notebook-optimized version (`modern_etl_notebook.py`) in Databricks
- Check for existing sessions before creating new ones

---

## ❌ **Error 3: DateTime Parsing Failures**

### **Error Messages:**
```
ERROR: Failed to parse timestamp 'yyyy-MM-dd hh:mm:ss aa'
DateTimeException: Unable to parse datetime
IllegalArgumentException: Invalid datetime format for Spark 3.x
```

### **Root Cause:**
Spark 3.5 uses Proleptic Gregorian calendar, which is incompatible with some legacy datetime patterns.

### **Solution:**
Update datetime parsing patterns:

```python
# ❌ DON'T DO THIS (legacy pattern, may fail in Spark 3.5)
df.withColumn("unix_ts", unix_timestamp(df["timestamp_str"], 'yyyy-MM-dd hh:mm:ss aa'))

# ✅ DO THIS INSTEAD (Spark 3.5 compatible)
df.withColumn(
    "parsed_timestamp",
    to_timestamp(col("timestamp_str"), "yyyy-MM-dd hh:mm:ss a")
).withColumn(
    "unix_ts",
    unix_timestamp(col("parsed_timestamp"))
)
```

### **Prevention:**
- Use `to_timestamp()` for explicit datetime parsing
- Test datetime patterns with Spark 3.5
- Use two-step parsing for better error handling

---

## ❌ **Error 4: Python UDF Performance Issues**

### **Error Messages:**
```
WARN: Python UDF detected - this may cause performance issues
Task taking longer than expected due to serialization overhead
```

### **Root Cause:**
Python UDFs are slow due to serialization overhead between JVM and Python.

### **Solution:**
Replace Python UDFs with built-in functions:

```python
# ❌ DON'T DO THIS (slow Python UDF)
def clean_text(s):
    if s:
        return s.strip().upper()
    return None

clean_udf = udf(clean_text, StringType())
df.withColumn("clean_text", clean_udf(df["text"]))

# ✅ DO THIS INSTEAD (fast built-in functions)
from pyspark.sql.functions import col, upper, trim, when, lit

df.withColumn(
    "clean_text",
    when(col("text").isNotNull(), upper(trim(col("text"))))
    .otherwise(lit(None))
)
```

### **Prevention:**
- Use built-in Spark functions whenever possible
- Consider Pandas UDFs for complex operations
- Profile performance to identify bottlenecks

---

## ❌ **Error 5: Import and Dependency Issues**

### **Error Messages:**
```
ModuleNotFoundError: No module named 'pyspark'
ImportError: cannot import name 'SparkSession'
```

### **Root Cause:**
Missing or incorrect PySpark installation.

### **Solution:**
Install correct PySpark version:

```bash
# Install PySpark 3.5
pip install pyspark==3.5.0

# Or use the quick test script
python quick_test.py  # Automatically installs if needed
```

### **Prevention:**
- Use virtual environments for dependency management
- Pin PySpark version in requirements.txt
- Test installation before running scripts

---

## ❌ **Error 6: Schema Inference Issues**

### **Error Messages:**
```
AnalysisException: Unable to infer schema for DataFrame
IllegalArgumentException: Schema mismatch
```

### **Root Cause:**
Spark 3.5 has stricter schema inference and type checking.

### **Solution:**
Define explicit schemas:

```python
# ❌ DON'T DO THIS (implicit schema inference)
df = spark.createDataFrame(data, ["col1", "col2"])

# ✅ DO THIS INSTEAD (explicit schema)
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("col1", StringType(), True),
    StructField("col2", StringType(), True)
])
df = spark.createDataFrame(data, schema)
```

### **Prevention:**
- Always define explicit schemas for production code
- Use schema evolution strategies for changing data
- Test schema compatibility across Spark versions

---

## 🔧 **General Troubleshooting Steps**

### **1. Check Spark Version**
```python
print(f"Spark version: {spark.version}")
```

### **2. Enable Debug Logging**
```python
spark.sparkContext.setLogLevel("DEBUG")
```

### **3. Check Configuration**
```python
# View current Spark configuration
for item in spark.sparkContext.getConf().getAll():
    print(item)
```

### **4. Test in Isolation**
- Run small test cases first
- Isolate problematic operations
- Use the `quick_test.py` script for local testing

### **5. Check Databricks Runtime**
- Use Databricks Runtime 13.0+ for Spark 3.5
- Enable serverless compute for best compatibility
- Check cluster configuration

---

## 📚 **Additional Resources**

- **[Complete Migration Journey](migration_journey.md)** - Full step-by-step process
- **[Migration Comparison](migration_comparison.md)** - Technical details
- **[Spark 3.5 Documentation](https://spark.apache.org/docs/3.5.0/)** - Official docs
- **[Databricks Migration Guide](https://docs.databricks.com/spark/latest/spark-sql/spark-sql-migration-guide.html)** - Platform-specific guidance

---

## 🆘 **Still Having Issues?**

1. **Check the error message** against this guide
2. **Run the quick test** to isolate the problem
3. **Review the migration journey** for similar issues
4. **Test with minimal code** to identify the root cause
5. **Check Spark and Databricks versions** for compatibility

**Remember: Most migration issues are common and have well-documented solutions! 🎯**