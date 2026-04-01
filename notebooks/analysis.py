from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Create Spark session
spark = SparkSession.builder.appName("CustomerAnalytics").getOrCreate()

# Load data
df = spark.read.csv("data/customer_data.csv", header=True, inferSchema=True)

print("Original Data:")
df.show()

# -------------------------------
# Data Cleaning
# -------------------------------
df_clean = df.dropna()

# -------------------------------
# Transformation
# -------------------------------

# Filter high spending customers
df_filtered = df_clean.filter(col("spend") > 1500)

# Average spend by city
df_grouped = df_filtered.groupBy("city").agg(avg("spend").alias("avg_spend"))

# -------------------------------
# Output
# -------------------------------
print("Transformed Data:")
df_grouped.show()

spark.stop()
