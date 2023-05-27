"""CW - Top10MostPopularServices- Part D- Misc Analysis- Gas Guzzlers
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
        .appName("GasGuzzler")\
        .getOrCreate()

    def gasguzzler_transactions(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            float(fields[9])
            float(fields[11])
            return True
        except:
            return False

    def gasguzzler_contracts(line):
        try:
            fields = line.split(',')
            if len(fields)!=6:
                return False
            else:
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
    contracts = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")

    
    transac = transactions.filter(gasguzzler_transactions)
    cons = contracts.filter(gasguzzler_contracts)

    
    def map1(line):
        [_, _, _, _, _, _, _, _, _,gas_price ,_, block_timestamp,_,_,_] = line.split(',')
        date = time.strftime("%m/%Y",time.gmtime(int(block_timestamp)))
        gp = float(gas_price)
        return (date, (gp, 1))

    tf = transac.map(map1)
    transac_reduce = tf.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    average = trans_reduce.sortByKey(ascending=True)
    average_gas_price = average.map(lambda a: (a[0], str(a[1][0]/a[1][1]))) 
    
    
    def map2(line):
        [_, _, _, _, _, _, to_address, _, gas,_,_, block_timestamp,_,_,_] = line.split(',')
        date = time.strftime("%m/%Y",time.gmtime(int(block_timestamp)))
        g = float(gas)
        s = str(to_address)
        return (s,(date, g))

    tf1 = transac.map(map2)
    cf = cons.map(lambda x: (x.split(',')[0],1))
    joins = tf1.join(cf)
    map_1 = joins.map(lambda x: (x[1][0][0], (x[1][0][1],x[1][1])))
    transac_reduce1 = map_1.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    average_gas_used = transac_reducing1.map(lambda a: (a[0], str(a[1][0]/a[1][1])))
    average_gas_used = average_gas_used.sortByKey(ascending = True)
    

    bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    result_object = bucket_resource.Object(s3_bucket,'ethereum_partd3_1_' + date_time + '/avg_gasprice.txt')
    result_object.put(Body=json.dumps(average_gas_price.take(100)))               
    result_object1 = my_bucket_resource.Object(s3_bucket,'ethereum_partd3_1_' + date_time + '/avg_gasused.txt')
    result_object1.put(Body=json.dumps(average_gas_used.take(100)))
    
    spark.stop()