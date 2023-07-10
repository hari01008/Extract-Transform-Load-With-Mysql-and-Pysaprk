from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import time

spark = SparkSession.builder \
    .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.22") \
    .getOrCreate()

# Start the timer
start_time = time.time()


# Read Parquet file  
df = spark.read.parquet("transformed_output.parquet")

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

# End the timer
end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time

# Print the elapsed time
print("Time taken: {} seconds".format(elapsed_time))