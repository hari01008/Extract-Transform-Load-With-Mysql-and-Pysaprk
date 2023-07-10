In this data processing pipeline, we extract data from a MySQL database and store it as a CSV file. Subsequently, we convert the CSV file to the Parquet format and perform transformations on the data. Finally, we obtain the transformed data from the Parquet format.

Here is a revised description of the steps involved:

1. Extraction: The process begins by extracting data from the MySQL database. This entails connecting to the database, executing appropriate SQL queries, and retrieving the data.

2. CSV Storage: Once the data is extracted, we store it in the CSV format. This step allows us to have a structured representation of the data that can be easily accessed and manipulated.

3. Conversion to Parquet: After storing the data as a CSV file, we convert it to the Parquet format. Parquet offers advantages such as efficient compression, columnar storage, and schema evolution support, making it a suitable choice for data processing and analysis.

4. Transformation: Once the data is in the Parquet format, we perform the required transformations on it. This step involves using PySpark or other compatible tools to apply operations such as filtering, aggregations, joins, or any other transformations necessary to derive the desired output.

   The operations performed :
   1)Date format change (mm-dd-yyyy)to(yyyy-mm-dd)
   2)concatenate data from two column to one column
   3)split string from one column to two column
   4)drop one or more columns

10. Output from Parquet: After the transformations are complete, we obtain the transformed data from the Parquet format. This data can be used for further analysis, visualization, or downstream processing.

By employing this pipeline, we can efficiently extract data from a MySQL database, store it in the CSV format for easy accessibility, convert it to the optimized Parquet format for efficient processing, perform transformations on the Parquet data, and obtain the desired output for subsequent use or analysis.


HOW TO RUN THE PROJECT :
1) First Install The Pyspark
2) Then With the dataset import the dataset to the mysql workbench
3)then install all the libraries required
4)then run the all program seperately (extract,transform,load)
5)if u want GUI run the (app.py)


REQUIRED LIBRARIES:
1)pyspark: Install using pip install pyspark.
2)mysql-connector-java: Download the MySQL Connector/J driver JAR file from the official MySQL website (https://dev.mysql.com/downloads/connector/j/) and add it to your project's classpath or specify the path to the JAR file in the code.
3)tkinter: It is a standard Python library for creating GUIs. Usually, it is already included with Python installations, but if you don't have it, you can install it using your package manager or follow platform-specific instructions.

DATASET :
i will provide a link for dataset

Contact:
EMAIL:haricse0808@gmail.com
