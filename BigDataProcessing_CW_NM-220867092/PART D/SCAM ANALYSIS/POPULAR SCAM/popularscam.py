"""CW - Top10MostPopularServices- Part D- Scam Analysis- Popular Scams- Most Lucrative
"""


import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("PopularScam")\
        .getOrCreate()

    def check_transactions(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            int(fields[11])
            str(fields[6])
            float(fields[7])
            return True
        except:
            return False
    
    def check_scams(line):
        try:
            fields = line.split(',')
            if len(fields)!=8:
                return False
            int(fields[0])
            str(fields[4])
            str(fields[6])
            return True
        except:
            return False

    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")  
    
    
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    scams = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/scams.csv")

    
    transac = transactions.filter(check_transactions)
    sc = scams.filter(check_scams)
    
    sf = sc.map(lambda l: (l.split(',')[6], (l.split(',')[0],l.split(',')[4])))
    transac_map = transac.map(lambda l:  (l.split(',')[6], float(l.split(',')[7])))
    join = transac_map.join(sf)

    map1 = join.map(lambda x: ((x[1][1][0], x[1][1][1]),x[1][0]))
    popular_scams= map1.reduceByKey(lambda a,b: a+b)
    popular_scams = popular_scams.map(lambda a: ((a[0][0],a[0][1]),float(a[1])))
    top10_ps = popular_scams.takeOrdered(15, key=lambda l: -1*l[1])
    print(popular_scams.take(10))
    
    
    bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    result_object = bucket_resource.Object(s3_bucket,'ethereum_partd1_' + date_time + '/most_lucrative_scams.txt')
    result_object.put(Body=json.dumps(top10_ps))            
    
    spark.stop()