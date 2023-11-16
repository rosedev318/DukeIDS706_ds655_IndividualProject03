# Databricks notebook source
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import pandas as pd

def validate_data(data):
    if not isinstance(data, pd.DataFrame):
        raise ValueError("Data should be a pandas DataFrame")
    if data.empty:
        raise ValueError("Data should not be empty")


def load_into_delta_lake(data, path):
    try:
        validate_data(data)

        # Convert pandas dataframe to spark dataframe
        spark = SparkSession.builder.getOrCreate()
        spark_df = spark.createDataFrame(data)

        # Write the DataFrame to a Delta Lake table (if the table already exists, overwrite it)
        spark_df.write.format("delta").mode("overwrite").saveAsTable(path)
    except ValueError as e:
        print(f"Invalid data: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Use the function
try:
    data = pd.read_csv("https://raw.githubusercontent.com/nogibjj/DukeIDS706_ds655_IndividualProject03/main/Data/Iris_Data.csv")
    load_into_delta_lake(data, "delta_table_iris")
except Exception as e:
    print(f"An error occurred: {e}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta_table_iris
