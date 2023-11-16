# Individual Project #3: Databricks ETL (Extract Transform Load) Pipeline


[![Install](https://github.com/nogibjj/DukeIDS706_ds655_IndividualProject03/actions/workflows/01_Install.yml/badge.svg)](https://github.com/nogibjj/DukeIDS706_ds655_IndividualProject03/actions/workflows/01_Install.yml)
[![Black Formatter](https://github.com/nogibjj/DukeIDS706_ds655_IndividualProject03/actions/workflows/02_Format.yml/badge.svg)](https://github.com/nogibjj/DukeIDS706_ds655_IndividualProject03/actions/workflows/02_Format.yml)
[![Lint](https://github.com/nogibjj/DukeIDS706_ds655_IndividualProject03/actions/workflows/03_Lint.yml/badge.svg)](https://github.com/nogibjj/DukeIDS706_ds655_IndividualProject03/actions/workflows/03_Lint.yml)
[![Test](https://github.com/nogibjj/DukeIDS706_ds655_IndividualProject03/actions/workflows/04_Test.yml/badge.svg)](https://github.com/nogibjj/DukeIDS706_ds655_IndividualProject03/actions/workflows/04_Test.yml)

# Components:

## 1 - A well-documented Databricks notebook that performs ETL (Extract, Transform, Load) operations, checked into the repository

Azure Workspace [Link](https://adb-2656694793182894.14.azuredatabricks.net/browse/folders/2685268812376476?o=2656694793182894)


## 2 - Usage of Delta Lake for data storage

File Name - `02_Delta_Lake_For_Storage.py` - an Azure Databricks Notebook that creates a Delta Table in the Delta Lake using *Spark*

 * Read the iris dataset from the Data Folder
 * Create a pandas DataFrame `data`
 * Convert the Pandas DataFrame to a Spark Dataframe `spark_df` (because only spark dataframes can be converted to a delta format)
 * Save the Spark DataFrame as a Delta-Table `delta_table_iris` (Overwrite mode is on so that if the table already exists, it will be re-written instead of giving errors)
 * Error handling at every step to highlight errors
![Delta Lake for Storage](https://github.com/nogibjj/DukeIDS706_ds655_IndividualProject03/blob/ac0b5d7704c4c6a488b37883534836f9fd4630c2/Resources/1116_Delta_Lake_For_Storage%20-%20Databricks.png)


## 3 - Usage of Spark SQL for data transformations

File Name - `03_Spark_SQL_For_DataTransformation.py` - an Azure Databricks Notebook that queries the Iris Delta Table created above using *Spark SQL*

 * Create a Spark Session using PySpark.SQL
 * Query the `delta_table_iris` table created in the above step (#2)
 * Using the *GROUP BY* command in SQL to transform the data and make it readable
 * Writing this data into a new delta table `iris_transformed` for future steps (visualization)
 * Error handling at every step
![Usage of Spark SQL](https://github.com/nogibjj/DukeIDS706_ds655_IndividualProject03/blob/ac0b5d7704c4c6a488b37883534836f9fd4630c2/Resources/1116_Spark_SQL_for_DataTransformations%20-%20Databricks.png)


## 4 - Proper error handling and data validation
Error handling and Data Validation is performed individually in every code and notebook. Errors are published wherever possible, and empty dataframes are flagged as well.

## 5 - Visualization of the transformed data

File Name - `04_Visualization_of_Transformed_Data.py` - an Azure Databricks Notebook that creates a Visualization based on the *transformed* delta table created in Step 03

 * Query the `iris_transformed` Delta Table usign Spark SQL
 * Generate a chart of the data using Matplotlib
 * Save the chart as `avg_sepal_length_by_species.png` in the Azure workspace
 * Location: `/dbfs/tmp/avg_sepal_length_by_species.png`
 * The image cannot be pushed into the Github Repository directly from a Databricks notebook, because it requires git commands and authentication information
![Visualization](https://github.com/nogibjj/DukeIDS706_ds655_IndividualProject03/blob/ac0b5d7704c4c6a488b37883534836f9fd4630c2/Resources/1116_Visualization_of_Transformed_Data%20-%20Databricks.png)

## 6 - An automated trigger to initiate the pipeline

## 7 - Video Demo - [Link]()









#
## File Index

Files in this repository include:


## 1. Readme
  The `README.md` file is a markdown file that contains basic information about the repository, what files it contains, and how to consume them


## 2. Requirements
  The `requirements.txt` file has a list of packages to be installed for any required project. Currently, my requirements file contains some basic python packages.


## 3. Codes
  This folder contains all the code files used in this repository - the files named "Test_" will be used for testing and the remaining will define certain functions


## 4. Resources
  -  This folder contains any other files relevant to this project. Currently, I have added -


## 5. CI/CD Automation Files


  ### 5(a). Makefile
  The `Makefile` contains instructions for installing packages (specified in `requirements.txt`), formatting the code (using black formatting), testing the code (running all the sample python code files starting with the term *'Check...'* ), and linting the code using pylint


  ### 5(b). Github Actions
  Github Actions uses the `main.yml` file to call the functions defined in the Makefile based on triggers such as push or pull. Currently, every time a change is pushed onto the repository, it runs the install packages, formatting the code, linting the code, and then testing the code functions


  ### 5(c). Devcontainer
  
  The `.devcontainer` folder mainly contains two files - 
  * `Dockerfile` defines the environment variables - essentially it ensures that all collaborators using the repository are working on the same environment to avoid conflicts and version mismatch issues
  * `devcontainer.json` is a json file that specifies the environment variables including the installed extensions in the virtual environment
