from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StudentDatasetAnalysis") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Load the dataset
csv_path = "students.csv"  # Update the path if needed
df_spark = spark.read.csv(csv_path, header=True, inferSchema=True)

# Show dataset structure
df_spark.printSchema()

# Perform basic statistics on numeric columns
df_spark.describe().show()

# Average GPA per department
df_spark.groupBy("Department").agg(avg("GPA").alias("AvgGPA")).show()

# Count students per graduation year
df_spark.groupBy("GraduationYear").agg(count("*").alias("StudentCount")).orderBy("GraduationYear").show()

# Stop Spark session
spark.stop()
