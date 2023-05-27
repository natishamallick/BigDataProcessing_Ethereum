"""CW - Top10MostPopularServices- Part D- Misc Analysis- Data Overhead
"""


import sys, string
import os
import socket
import time
import io
import operator
import boto3
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import length, mean
from pyspark.sql import Row
from pyspark.sql.types import StringType, DoubleType, StructField, StructType

if __name__ == "__main__":

    
    spark = SparkSession\
        .builder\
        .appName("DataOverhead")\
        .getOrCreate()

    
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
 
    blocks = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv",
                           header=True, inferSchema=True)
    
   
    logs_bloom = blocks.select(mean(length("logs_bloom"))).collect()[0][0]
    sha3_uncles = blocks.select(mean(length("sha3_uncles"))).collect()[0][0]
    transactions_root = blocks.select(mean(length("transactions_root"))).collect()[0][0]
    state_root = blocks.select(mean(length("state_root"))).collect()[0][0]
    receipts_root = blocks.select(mean(length("receipts_root"))).collect()[0][0]

    byte_logs_bloom = (logs_bloom - 2) * 4 / 8
    byte_sha3_uncles = (sha3_uncles - 2) * 4 / 8
    byte_transactions_root = (transactions_root - 2) * 4 / 8
    byte_state_root = (state_root - 2) * 4 / 8
    byte_receipts_root = (receipts_root - 2) * 4 / 8

    
    total_rows = blocks.count()

    total_space_saved = [
        Row(column_name="logs_bloom", space_saved=byte_logs_bloom * total_rows),
        Row(column_name="sha3_uncles", space_saved=byte_sha3_uncles * total_rows),
        Row(column_name="transactions_root", space_saved=byte_transactions_root * total_rows),
        Row(column_name="state_root", space_saved=byte_state_root * total_rows),
        Row(column_name="receipts_root", space_saved=byte_receipts_root * total_rows)
    ]
    
  
    schema = StructType([
        StructField("column_name", StringType(), True),
        StructField("size_bytes", DoubleType(), True)
    ])

   
    df_results = spark.createDataFrame(total_space_saved, schema)

    
    all_columns = blocks.columns
    mean_lengths = blocks.select(*[mean(length(column)).alias(column) for column in all_columns]).collect()[0]

   
    column_bytes = [(column, ((mean_lengths[column] - 2) * 4 / 8) if mean_lengths[column] is not None else 0.0) for column in all_columns]

   
    total_rows = blocks.count()

   
    total_saved_space = [
        Row(column_name=column, space_saved=bytes * total_rows)
        for column, bytes in column_bytes
    ]

   
    total_occupied_space = sum([row.space_saved for row in total_saved_space])

   
    percentage_space_saved = [
        Row(column_name=row.column_name,
            space_saved=row.space_saved,
            percentage_saved=(row.space_saved / total_occupied_space) * 100)
        for row in total_saved_space
    ]

   
    percentage_schema = StructType([
        StructField("column_name", StringType(), True),
        StructField("space_saved", DoubleType(), True),
        StructField("percentage_saved", DoubleType(), True)
    ])

    percentage_results_df = spark.createDataFrame(percentage_space_saved, percentage_schema)

   
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H_%M_%S")

    
    overhead_data_file = f"overhead_{date_time}.txt"
    percentage_saved_file = f"percentage_saved_{date_time}.txt"

    
    csv_buffer = io.StringIO()
    df_results.toPandas().to_csv(csv_buffer, index=False, header=True, sep=',', line_terminator='\n')
    csv_buffer_percentage = io.StringIO()
    percentage_results_df.toPandas().to_csv(csv_buffer_percentage, index=False, header=True, sep=',', line_terminator='\n')
    
   
    bucket_resource = boto3.resource('s3',
                                        endpoint_url='http://' + s3_endpoint_url,
                                        aws_access_key_id=s3_access_key_id,
                                        aws_secret_access_key=s3_secret_access_key)

    result_object = bucket_resource.Object(s3_bucket,'ethereum_partd4_1_' + date_time + '/overhead_data_file.txt')
    result_object.put(Body=csv_buffer.getvalue())
    result_object = bucket_resource.Object(s3_bucket,'ethereum_partd4_1_' + date_time + '/percentage_saved_file.txt')
    result_object.put(Body=csv_buffer_percentage.getvalue())

   
    spark.stop()