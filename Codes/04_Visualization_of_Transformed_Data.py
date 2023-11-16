# Databricks notebook source
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

def validate_and_plot_data(result):
    # Validate result
    if result.rdd.isEmpty():
        raise ValueError("Query returned no results")

    # Convert the Spark DataFrame to a Pandas DataFrame
    result_pd = result.toPandas()

    # Validate pandas DataFrame
    if 'species' not in result_pd.columns or 'avg_sepal_length' not in result_pd.columns:
        raise ValueError("Expected columns are missing in the DataFrame")

    # Plot the average sepal length for each species
    result_pd.plot(kind='bar', x='species', y='avg_sepal_length', color='green')
    plt.title('Average Sepal Length by Species')
    plt.xlabel('Species')
    plt.ylabel('Average Sepal Length')
    plt.savefig('/dbfs/tmp/avg_sepal_length_by_species.png')
    plt.show()

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

try:
    result = spark.sql(
        """
        SELECT *
        FROM iris_transformed 
        """
    )
    validate_and_plot_data(result)
except AnalysisException as e:
    print(f"SQL query error: {e}")
except ValueError as e:
    print(f"Data validation error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
