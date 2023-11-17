# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
import pandas as pd

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

try:
    # EXTRACT
    # Perform ETL operations
    df = pd.read_csv("https://raw.githubusercontent.com/nogibjj/DukeIDS706_ds655_IndividualProject03/main/Data/Iris_Data.csv")
    print("Data Extraction: Done")

    # TRANSFORM
    spark_df = spark.createDataFrame(df)
    # Remove any rows with missing data
    spark_df = spark_df.dropna()
    # Convert the 'age' column to integer type
    spark_df = spark_df.withColumn("sepal_length", col("sepal_length").cast("integer"))
    print("Data Transformation: Done")

    # LOAD
    # Write data into a Delta table
    spark_df.write.format("delta").mode("overwrite").saveAsTable("Delta_Table")
    print("Data Loading: Done")

except AnalysisException as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
