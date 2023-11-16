# Databricks notebook source
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Use Spark SQL for transformations
result = spark.sql("""
    SELECT 
        species, 
        AVG(sepal_length) as avg_sepal_length, 
        AVG(sepal_width) as avg_sepal_width, 
        AVG(petal_length) as avg_petal_length, 
        AVG(petal_width) as avg_petal_width 
    FROM delta_table_iris 
    GROUP BY species
""")

# Show the result
result.show()
