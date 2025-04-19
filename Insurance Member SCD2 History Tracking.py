# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Mock initial data with datetime objects
data = [
    (101, "Alice Smith", "123 Main St", datetime(2024, 4, 1, 10, 0, 0)),
    (102, "Bob Jones", "456 Elm St", datetime(2024, 4, 1, 10, 0, 0))
]

schema = StructType([
    StructField("member_id", IntegerType(), True),
    StructField("member_name", StringType(), True),
    StructField("member_address", StringType(), True),
    StructField("last_updated", TimestampType(), True)
])

# Create DataFrame
raw_df = spark.createDataFrame(data, schema)
raw_df.createOrReplaceTempView("raw_member_feed")

# Show it
raw_df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the Database

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS insurance_db")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Empty Target Table for SCD2

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS insurance_db.dim_member_history (
    surrogate_key BIGINT GENERATED ALWAYS AS IDENTITY,
    member_id INT,
    member_name STRING,
    member_address STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

spark.sql("SHOW TABLES IN insurance_db").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the Dimension and Raw data

# COMMAND ----------

dim_df = spark.table("insurance_db.dim_member_history")

raw_df = spark.table("raw_member_feed")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filter Only Active Records from Dimension

# COMMAND ----------

active_dim_df = dim_df.filter("is_current = true")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join Raw Feed to Active Dimension on member_id

# COMMAND ----------

raw_df_alias = raw_df.alias("raw")
active_dim_df_alias = active_dim_df.alias("dim")

# Left Join
joined_df = raw_df_alias.join(active_dim_df_alias, on="member_id", how="left")

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Identify Records That Need to Be Expired

# COMMAND ----------

records_to_expire = joined_df.filter(
    (joined_df["raw.member_address"] != joined_df["dim.member_address"]) &
    (joined_df["dim.is_current"] == True)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Expire Old Records (Set is_current = False and update valid_to)

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

# Only update if needed
if records_to_expire.count() > 0:
    expired_records = records_to_expire.withColumn("is_current", lit(False)) \
                                       .withColumn("valid_to", current_timestamp()) \
                                       .withColumn("updated_at", current_timestamp()) \
                                       .select(dim_df.columns)  # match exact schema
else:
    expired_records = None

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare New Records (Insert Latest Version)

# COMMAND ----------

new_records = raw_df.withColumn("valid_from", current_timestamp()) \
                    .withColumn("valid_to", lit(None).cast("timestamp")) \
                    .withColumn("is_current", lit(True)) \
                    .withColumn("created_at", current_timestamp()) \
                    .withColumn("updated_at", current_timestamp()) \
                    .select(
                        "member_id", "member_name", "member_address",
                        "valid_from", "valid_to", "is_current",
                        "created_at", "updated_at"
                    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Merge Expired and New Records into Target Dimension Table

# COMMAND ----------

if expired_records:
    final_df = expired_records.unionByName(new_records)
else:
    final_df = new_records

# Write to Delta Table (Append mode)
final_df.write.format("delta").mode("append").saveAsTable("insurance_db.dim_member_history")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from insurance_db.dim_member_history;

# COMMAND ----------

