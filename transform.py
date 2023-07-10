import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit, split, to_date, date_format
import os
import time

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Start the timer
start_time = time.time()

# Load the config file
with open('config.json') as f:
    config = json.load(f)

#Check if the input_csv file exists and is in csv format
if 'input_csv' not in config or not config['input_csv'].endswith('.csv') or not os.path.exists(config['input_csv']):
    print("No CSV file is selected or the file is invalid.")
    exit()

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

# End the timer
end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time

# Print the elapsed time
print("Time taken: {} seconds".format(elapsed_time))
