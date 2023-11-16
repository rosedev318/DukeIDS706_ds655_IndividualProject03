# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()


def validate_and_execute_query():
    try:
        # Use Spark SQL for transformations
        result = spark.sql(
            """
            SELECT 
                species, 
                AVG(sepal_length) as avg_sepal_length, 
                AVG(sepal_width) as avg_sepal_width, 
                AVG(petal_length) as avg_petal_length, 
                AVG(petal_width) as avg_petal_width 
            FROM delta_table_iris 
            GROUP BY species
        """
        )

        # Validate result
        if result.rdd.isEmpty():
            raise ValueError("Query returned no results")
        
        # Write the result to a Delta table
        result.write.format("delta").mode("overwrite").saveAsTable("iris_transformed")

        # Show the result
        result.show()

    except AnalysisException as e:
        print(f"SQL query error: {e}")
    except ValueError as e:
        print(f"Data validation error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# Run the function
validate_and_execute_query()
