# MySQL Data Extraction, Transformation, and Loading (ETL) Pipeline with PySpark

Explore an end-to-end ETL pipeline where we extract data from a MySQL database, store it as CSV, transform it using PySpark, and obtain the final output from Parquet format.

## Overview

This data processing pipeline encompasses the following steps:

1. **Extraction:** Connect to a MySQL database, execute SQL queries, and retrieve data for processing.

2. **CSV Storage:** Store the extracted data in CSV format for structured representation.

3. **Conversion to Parquet:** Convert the CSV data to Parquet format, leveraging its efficiency and columnar storage benefits.

4. **Transformation:** Use PySpark for data transformation operations, such as date format changes, concatenation, splitting, and column dropping.

5. **Output from Parquet:** Obtain the transformed data from Parquet format, ready for analysis or downstream processing.

## Transformation Operations

The transformation phase involves the following operations:

1. Date Format Change: Convert dates from mm-dd-yyyy to yyyy-mm-dd.
2. Column Concatenation: Combine data from two columns into a single column.
3. String Split: Divide a string from one column into two columns.
4. Column Dropping: Remove one or more columns from the dataset.

## How to Run the Project

1. **Install PySpark:** Begin by installing PySpark using `pip install pyspark`.

2. **Setup MySQL:** Import the provided dataset into MySQL Workbench.

3. **Install Required Libraries:** Ensure you have `mysql-connector-java` and `tkinter` libraries installed. Download the MySQL Connector/J driver JAR file from the official MySQL website and add it to your classpath.

4. **Run Programs:** Execute each program separately: `extract.py` for data extraction, `transform.py` for transformations, and `load.py` for loading data into Parquet.

5. **GUI Option:** To use a GUI interface, run `app.py`.

## Dataset

For access to the dataset, please contact me.

## Contact

Feel free to reach out with any questions or collaboration opportunities at haricse0808@gmail.com.
