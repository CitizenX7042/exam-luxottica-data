# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

from datetime import datetime

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- ==============================
# MAGIC -- Luxottica Data Platform Setup
# MAGIC -- ==============================
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS luxottica;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS luxottica.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS luxottica.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS luxottica.gold;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS luxottica.bronze.raw_files;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS luxottica.gold.file_ingestion_tracker (
# MAGIC     file_name STRING,
# MAGIC     file_path STRING,
# MAGIC     file_size BIGINT,
# MAGIC     batch_tag STRING,
# MAGIC     status STRING,
# MAGIC     processed_at TIMESTAMP,
# MAGIC     last_updated TIMESTAMP,
# MAGIC     record_count BIGINT,
# MAGIC     error_message STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC

# COMMAND ----------

raw_path = "/Volumes/luxottica/bronze/raw_files/"

files = dbutils.fs.ls(raw_path)

csv_files = [f for f in files if f.name.endswith(".csv")]

print("CSV files found:")
for f in csv_files:
    print(f.name)


# COMMAND ----------

def extract_batch_tag(file_name):
    return file_name.split("_")[0]

for f in csv_files:
    print(f.name, "→", extract_batch_tag(f.name))



# COMMAND ----------



tracker_df = spark.table("luxottica.gold.file_ingestion_tracker")

existing_df = tracker_df.filter(
    F.col("status").isin("SUCCESS", "PROCESSING")
)

existing_files = [
    row.file_name
    for row in existing_df.select("file_name").collect()
]
print("Already processed files:")
print(existing_files)


# COMMAND ----------

new_files = [f for f in csv_files if f.name not in existing_files]

print("New files to process:")
for f in new_files:
    print(f.name)


# COMMAND ----------

if not new_files:
    dbutils.notebook.exit("No new files found. Nothing to process.")


# COMMAND ----------

pending_rows = [
    Row(
        file_name=f.name,
        file_path=f.path,
        file_size=f.size,
        batch_tag=extract_batch_tag(f.name),
        status="PROCESSING",
        processed_at=datetime.now(),
        last_updated=datetime.now(),
        record_count=None,
        error_message=None
    )
    for f in new_files
]


# COMMAND ----------

# DBTITLE 1,Create DataFrame for Pending Rows


if pending_rows:
    schema = StructType([
        StructField("file_name", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("file_size", LongType(), True),
        StructField("batch_tag", StringType(), True),
        StructField("status", StringType(), True),
        StructField("processed_at", TimestampType(), True),
        StructField("last_updated", TimestampType(), True),   # ← added
        StructField("record_count", LongType(), True),
        StructField("error_message", StringType(), True)
    ])

    pending_df = spark.createDataFrame(pending_rows, schema=schema)
    pending_df.display()


# COMMAND ----------

if pending_rows:
    pending_df.createOrReplaceTempView("new_files")

    spark.sql("""
        MERGE INTO luxottica.gold.file_ingestion_tracker t
        USING new_files s
        ON t.file_path = s.file_path
        WHEN NOT MATCHED THEN INSERT *
    """)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from luxottica.gold.file_ingestion_tracker

# COMMAND ----------

# Get distinct batch tags from the newly inserted rows
distinct_tags = pending_df.select("batch_tag").distinct().collect()

print(distinct_tags)

for row in distinct_tags:
    tag = row["batch_tag"]
    
    dbutils.notebook.run(
        "/Workspace/Users/itay.hay999@gmail.com/luxottica-processing",   # <-- replace with your real notebook path
        0,
        {"batch_tag": tag}
    )


# COMMAND ----------

# MAGIC %md
# MAGIC TESTING

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from luxottica.gold.file_ingestion_tracker
# MAGIC -- delete from luxottica.gold.file_ingestion_tracker --where status = 'PENDING'
# MAGIC -- update luxottica.gold.file_ingestion_tracker set status = 'PENDING' where file_name = 'orders_raw_data_batch2.csv'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from luxottica.silver.orders
# MAGIC -- delete from luxottica.silver.orders

# COMMAND ----------

# dbutils.fs.cp(
#     "dbfs:/Volumes/luxottica/raw/raw_files/orders_raw_data.csv",
#     "dbfs:/Volumes/luxottica/raw/raw_files/orders_raw_data_batch3.csv"
# )
