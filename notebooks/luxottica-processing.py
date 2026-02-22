# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType,DoubleType,IntegerType
from datetime import datetime
from pyspark.sql.window import Window
# from itertools import chain

# COMMAND ----------

dbutils.widgets.text("batch_tag", "orders")   # default only for manual runs
batch_tag = dbutils.widgets.get("batch_tag")

print("Processing batch_tag:", batch_tag)

# COMMAND ----------

tracker_df = spark.table("luxottica.gold.file_ingestion_tracker")
# display(tracker_df)
pending_files_df = tracker_df.filter(
    (F.col("status") == "PROCESSING") &
    (F.col("batch_tag") == batch_tag)
)

pending_files_df.display()


# COMMAND ----------

pending_files = [row.file_path for row in pending_files_df.select("file_path").collect()]

print("Files to process:")
for f in pending_files:
    print(f)

if not pending_files:
    dbutils.notebook.exit("No pending files for this batch.")


# COMMAND ----------

df = spark.read \
    .option("header", True) \
    .option("multiLine", True) \
    .option("escape", '"') \
    .option("quote", '"') \
    .csv(pending_files)

df.printSchema()
df.display()


# COMMAND ----------


customer_schema = StructType([
    StructField("name", StringType(), True),
    StructField("email", StringType(), True)
])

payload_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer", customer_schema, True),
    StructField("product", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("status", StringType(), True),
    StructField("order_time", StringType(), True)
])


# COMMAND ----------


df_parsed = df.withColumn(
    "parsed_payload",
    F.from_json(F.col("raw_payload"), payload_schema)
)

cnt =df_parsed.filter(F.col("parsed_payload").isNull()).count()
print(cnt)

df_parsed.printSchema()
df_parsed.display()


# COMMAND ----------

df_flat = df_parsed.select(
    "record_id",
    F.col("parsed_payload.order_id").alias("order_id"),
    F.col("parsed_payload.customer.name").alias("customer_name"),
    F.col("parsed_payload.customer.email").alias("email"),
    F.col("parsed_payload.product").alias("product"),
    F.col("parsed_payload.price").alias("price"),
    F.col("parsed_payload.quantity").alias("quantity"),
    F.col("parsed_payload.city").alias("city"),
    F.col("parsed_payload.status").alias("status"),
    F.col("parsed_payload.order_time").alias("order_time")
)

df_flat.printSchema()
display(df_flat)


# COMMAND ----------

# Remove Missing order_id

df_clean = df_flat.filter(F.col("order_id").isNotNull())
# deal with empty string
df_clean = df_clean.filter(F.trim(F.col("order_id")) != "") 

df_clean.display()


# COMMAND ----------

# Standardize Status : Fail,Completed,PENDING,pending
df_clean = df_clean.withColumn(
    "status",
    F.lower(F.trim(F.col("status")))
)

df_clean = df_clean.withColumn(
    "status",
    F.when(F.col("status").isin("completed"), "completed")
     .when(F.col("status").isin("pending"), "pending")
     .when(F.col("status").isin("fail", "failed"), "failed")
     .otherwise("pending")
)

df_clean.display()

# COMMAND ----------

# replace null quantity with 1
df_clean = df_clean.withColumn(
    "quantity",
    F.when(F.col("quantity").isNull(), 1)
     .otherwise(F.col("quantity"))
)

df_clean.display()


# COMMAND ----------

# try_to_timestamp will return NULL for '01-32-2024' without crashing

# df_final = df_clean.withColumn(
#     "order_time", 
#     F.coalesce(
#         F.try_to_timestamp(F.col("order_time"), F.lit("yyyy-MM-dd HH:mm:ss")),
#         F.try_to_timestamp(F.col("order_time"), F.lit("MM-dd-yyyy"))
#     )
# )
# df_final.show()
# df_final.display()

df_final = df_clean.withColumn(
    "order_time",
    F.coalesce(
        F.try_to_timestamp(F.col("order_time"), F.lit("yyyy/MM/dd HH:mm:ss")),
        F.try_to_timestamp(F.col("order_time"), F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(F.col("order_time"), F.lit("MM/dd/yyyy HH:mm:ss"))
    )
)


df_final.show()
df_final.display()



# COMMAND ----------

# Deduplicate results based on order_id
window_spec = Window.partitionBy("order_id") \
    .orderBy(F.col("order_time").desc_nulls_last())

df_dedup = df_final.withColumn(
    "row_num",
    F.row_number().over(window_spec)
).filter(
    F.col("row_num") == 1
).drop("row_num")

df_dedup.display()


# COMMAND ----------

df_output = df_dedup.select(
    "order_id",
    "customer_name",
    "email",
    "product",
    "price",
    "quantity",
    "city",
    "status",
    "order_time"
)

df_output.display()


# COMMAND ----------


try:
    # ---- WRITE TO SILVER ----
    df_output.createOrReplaceTempView("source_orders")

    spark.sql("""
    MERGE INTO luxottica.silver.orders t
    USING source_orders s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

    final_record_count = df_output.count()

    # ---- SUCCESS UPDATE ----
    update_rows = [
        (
            row.file_name,
            datetime.now(),
            final_record_count
        )
        for row in pending_files_df.select("file_name").collect()
    ]

    update_schema = StructType([
        StructField("file_name", StringType(), True),
        StructField("processed_at", TimestampType(), True),
        StructField("record_count", LongType(), True)
    ])

    updates_df = spark.createDataFrame(update_rows, schema=update_schema)
    updates_df.createOrReplaceTempView("updates")

    spark.sql("""
        MERGE INTO luxottica.gold.file_ingestion_tracker t
        USING updates u
        ON t.file_name = u.file_name
        WHEN MATCHED THEN UPDATE SET
            t.status = 'SUCCESS',
            t.processed_at = u.processed_at,
            t.last_updated = current_timestamp(),
            t.record_count = u.record_count,
            t.error_message = NULL
    """)

    print("Silver write + SUCCESS update completed.")

except Exception as e:

    error_message = str(e)
    print("Write failed:", error_message)

    fail_rows = [
        (
            row.file_name,
            error_message
        )
        for row in pending_files_df.select("file_name").collect()
    ]

    fail_schema = StructType([
        StructField("file_name", StringType(), True),
        StructField("error_message", StringType(), True)
    ])

    fail_df = spark.createDataFrame(fail_rows, schema=fail_schema)
    fail_df.createOrReplaceTempView("fail_updates")

    spark.sql("""
        MERGE INTO luxottica.gold.file_ingestion_tracker t
        USING fail_updates f
        ON t.file_name = f.file_name
        WHEN MATCHED THEN UPDATE SET
            t.status = 'FAILED',
            t.error_message = f.error_message,
            t.last_updated = current_timestamp()
    """)

    raise


# COMMAND ----------

# MAGIC %md
# MAGIC ################################################################################################################################
# MAGIC ## validate results
# MAGIC ################################################################################################################################

# COMMAND ----------

df_dedup.groupBy("order_id").count().filter("count > 1").show()


# COMMAND ----------

# final_record_count = df_output.count()
# print("Final record count:", final_record_count)
# print("Total records:", df_output.count())

print("Final record count:", df_output.count())
print("Distinct orders:", df_output.select("order_id").distinct().count())
print("Invalid timestamps:", df_output.filter(F.col("order_time").isNull()).count())


# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from luxottica.silver.orders
# MAGIC -- delete from luxottica.silver.orders
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from luxottica.silver.orders