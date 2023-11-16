# Databricks notebook source
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import pandas as pd


def load_into_delta_lake(data, path):
    # Convert pandas dataframe to spark dataframe
    spark = SparkSession.builder.getOrCreate()
    spark_df = spark.createDataFrame(data)

    # Write the DataFrame to a Delta Lake table (if the table already exists, overwrite it)
    spark_df.write.format("delta").saveAsTable(path, mode="Overwrite")


# Use the function
data = pd.read_csv(
    "https://raw.githubusercontent.com/nogibjj/DukeIDS706_ds655_IndividualProject03/main/Data/Iris_Data.csv"
)
load_into_delta_lake(data, "delta_table_iris")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_table_iris
