import json
import os
from tkinter import *
from tkinter import filedialog
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit, split, to_date, date_format
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


def perform_extract():
    # Read configuration from config.json file
    with open("config.json") as config_file:
        config = json.load(config_file)

    mysql_url = config["mysql_url"]
    mysql_properties = config["mysql_properties"]
    output_dir = config["output_dir"]

    # Set up SparkSession
    spark = SparkSession.builder \
        .appName("MySQL to CSV") \
        .config("spark.driver.extraClassPath", "C:/Program Files (x86)/MySQL/Connector J 8.0/mysql-connector-j-8.0.33.jar") \
        .getOrCreate()

    # Fetch the table names from the INFORMATION_SCHEMA
    table_names = spark.read \
        .format("jdbc") \
        .option("url", mysql_url) \
        .option("user", mysql_properties["user"]) \
        .option("password", mysql_properties["password"]) \
        .option("driver", mysql_properties["driver"]) \
        .option("dbtable", "(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'hari') AS tables") \
        .load()

    # Convert the table names to a list
    table_names_list = [row["TABLE_NAME"] for row in table_names.collect()]

    # Extract data for each table
    for table_name in table_names_list:
        # Read data from MySQL database
        df = spark.read \
            .format("jdbc") \
            .option("url", mysql_url) \
            .option("dbtable", table_name) \
            .option("user", mysql_properties["user"]) \
            .option("password", mysql_properties["password"]) \
            .option("driver", mysql_properties["driver"]) \
            .load()

        # Specify the output CSV file path with table name
        csv_file_path = os.path.join(output_dir, "extracted", f"{table_name}.csv")

        # Write DataFrame to CSV
        df.write \
            .format("csv") \
            .option("header", "true") \
            .mode("overwrite") \
            .csv(csv_file_path)

    # Stop the Spark session
    spark.stop()


def perform_transform():
    # Load the config file
    with open('config.json') as f:
        config = json.load(f)

    # Check if the input_csv file exists and is in csv format
    if 'input_csv' not in config or not config['input_csv'].endswith('.csv') or not os.path.exists(config['input_csv']):
        print("No CSV file is selected or the file is invalid.")
        return

    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Load the DataFrame from the CSV file
    df = spark.read.csv(config['input_csv'], header=True, inferSchema=True)

    # Convert the DataFrame to a Parquet file
    df.write.mode("overwrite").parquet('temp.parquet')

    # Load the DataFrame from the Parquet file
    df = spark.read.parquet('temp.parquet')

    # Apply transformations
    for transformation in config['transformations']:
        if transformation['type'] == 'date_format':
            df = df.withColumn(transformation['column'], to_date(col(transformation['column']), transformation['input_format']))
            df = df.withColumn(transformation['column'], date_format(col(transformation['column']), transformation['output_format']))
        elif transformation['type'] == 'concat':
            df = df.withColumn(transformation['output_column'], concat(*[col(c) for c in transformation['columns']], lit(transformation['separator'])))
        elif transformation['type'] == 'split':
            split_col = split(df[transformation['column']], transformation['separator'])
            for i, output_column in enumerate(transformation['output_columns']):
                df = df.withColumn(output_column, split_col.getItem(i))
        elif transformation['type'] == 'drop':
            df = df.drop(transformation['column'])

    # Get the total number of records
    total_records = df.count()

    # Print the total number of records
    print("Total records: ", total_records)

    # Save the transformed DataFrame to a new Parquet file
    df.write.mode('overwrite').parquet(config['output_parquet'])
    df = spark.read.parquet(config['output_parquet'])
    df.show()

    # Stop the Spark session
    spark.stop()


def perform_load():
    # Load the config file
    with open('config.json') as f:
        config = json.load(f)

    # Create a SparkSession
    spark = SparkSession.builder \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.22") \
        .getOrCreate()

    # Read Parquet file
    df = spark.read.parquet(config['output_parquet'])

    # Define MySQL properties
    properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Renumber rows starting from 1
    window = Window.orderBy("User Id")
    df = df.withColumn("sno", row_number().over(window))

    # Write to MySQL table
    df.write.jdbc(url="jdbc:mysql://localhost:3306/hari1",
                  table="trasform_parquet",
                  mode="overwrite",
                  properties=properties)

    # Stop the Spark session
    spark.stop()


# Set up the GUI
root = Tk()
root.title("ETL App")

# Extract Button
extract_button = Button(root, text="Extract", command=perform_extract)
extract_button.pack()

# Transform Button
transform_button = Button(root, text="Transform", command=perform_transform)
transform_button.pack()

# Load Button
load_button = Button(root, text="Load", command=perform_load)
load_button.pack()

# Start the Tkinter event loop
root.mainloop()
