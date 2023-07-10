import os
import json
from pyspark.sql import SparkSession
import time

# Set up SparkSession
spark = SparkSession.builder \
    .appName("Extract") \
    .config("spark.driver.extraClassPath", "C:/Program Files (x86)/MySQL/Connector J 8.0/mysql-connector-j-8.0.33.jar") \
    .getOrCreate()

# Start the timer
start_time = time.time()

# Read configuration from config.json file
with open("config.json") as config_file:
    config = json.load(config_file)

mysql_url = config["mysql_url"]
mysql_properties = config["mysql_properties"]
output_dir = config["output_dir"]

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

# End the timer
end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time

# Print the elapsed time
print("Time taken: {} seconds".format(elapsed_time))

# Stop the Spark session
spark.stop()