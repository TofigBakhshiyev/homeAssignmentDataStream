import os
import sys
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
import dbConnection 
import writeToS3

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable 

# a SparkSession
spark = SparkSession.builder \
    .appName("PySpark Creditstar home assignment") \
    .config("spark.jars", "postgresql-42.7.1.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# db config
jdbc_config = dbConnection.connect()

# Loading loan and payment tables from PostgreSQL
loan_df = spark.read.format("jdbc").options(
    **jdbc_config,
    dbtable="loan"
).load()

payment_df = spark.read.format("jdbc").options(
    **jdbc_config,
    dbtable="payment"
).load()

# the client paid loans count
paid_loans_count = loan_df.filter(loan_df["status"] == "paid") \
    .groupBy("client_id").agg(F.count("*").alias("paid loans count"))

# the days count since last payment 
late_payments_df = payment_df.join(loan_df, payment_df["loan_id"] == loan_df["id"]) \
    .filter(payment_df["status"] == "late") \
    .groupBy("client_id").agg(F.max(F.datediff(F.current_date(), payment_df["created_on"])).alias("days since last late payment count"))

result = paid_loans_count.join(late_payments_df, "client_id", "left")

# Convert DataFrame to CSV string
resultsPandas = result.toPandas().to_csv(index=False)

writeToS3.write(resultsPandas)

# Stop the SparkSession
spark.stop()
