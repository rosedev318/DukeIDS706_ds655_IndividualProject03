# Databricks notebook source
df = (
    spark.read.format("csv")
    .option("sep", "\t")
    .load("dbfs:/databricks-datasets/songs/data-001/part-00000")
)
df.display()

# COMMAND ----------

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField

# Define variables used in the code below
file_path = "/databricks-datasets/songs/data-001/"
table_name = "songs"
checkpoint_path = "/tmp/pipeline_get_started/_checkpoint/song_data"

schema = StructType(
  [
    StructField("artist_id", StringType(), True),
    StructField("artist_lat", DoubleType(), True),
    StructField("artist_long", DoubleType(), True),
    StructField("artist_location", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("end_of_fade_in", DoubleType(), True),
    StructField("key", IntegerType(), True),
    StructField("key_confidence", DoubleType(), True),
    StructField("loudness", DoubleType(), True),
    StructField("release", StringType(), True),
    StructField("song_hotnes", DoubleType(), True),
    StructField("song_id", StringType(), True),
    StructField("start_of_fade_out", DoubleType(), True),
    StructField("tempo", DoubleType(), True),
    StructField("time_signature", DoubleType(), True),
    StructField("time_signature_confidence", DoubleType(), True),
    StructField("title", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("partial_sequence", IntegerType(), True)
  ]
)

(spark.readStream
  .format("cloudFiles")
  .schema(schema)
  .option("cloudFiles.format", "csv")
  .option("sep","\t")
  .load(file_path)
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name)
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM songs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE
# MAGIC   songs_smaller (
# MAGIC     artist_id STRING,
# MAGIC     artist_name STRING,
# MAGIC     duration DOUBLE,
# MAGIC     release STRING,
# MAGIC     tempo DOUBLE,
# MAGIC     time_signature DOUBLE,
# MAGIC     title STRING,
# MAGIC     year DOUBLE,
# MAGIC     processed_time TIMESTAMP
# MAGIC   );
# MAGIC
# MAGIC INSERT INTO
# MAGIC   songs_smaller
# MAGIC SELECT
# MAGIC   artist_id,
# MAGIC   artist_name,
# MAGIC   duration,
# MAGIC   release,
# MAGIC   tempo,
# MAGIC   time_signature,
# MAGIC   title,
# MAGIC   year,
# MAGIC   current_timestamp()
# MAGIC FROM
# MAGIC   songs
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import pandas as pd

def load_into_delta_lake(data, path):
    # Convert pandas dataframe to spark dataframe
    spark = SparkSession.builder.getOrCreate()
    spark_df = spark.createDataFrame(data)

    # Write the DataFrame to a Delta Lake table
    spark_df.write.format("delta").saveAsTable(path)
# Use the function
data = pd.read_csv("https://raw.githubusercontent.com/nogibjj/DukeIDS706_ds655_IndividualProject03/main/Data/Iris_Data.csv")
load_into_delta_lake(data, 'delta_table')

# COMMAND ----------


