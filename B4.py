print('Producing messages.....')

import requests, os
from kafka import KafkaProducer
from pyspark.sql import SparkSession, Row
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
def kafka_prod():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/Network.csv")

    
    data_list = [data for data in response.text.splitlines()[1:]]
    for data in data_list:
        producer.send('network1.3', data.encode('utf-8'))
    producer.flush()
print('Consuming messages......')        
def spark_kafka():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
    spark = SparkSession.builder.getOrCreate()
    
    raw_kafka_in_df = spark.readStream \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", "localhost:9092") \
                        .option("subscribe", 'network1.3') \
                        .option("startingOffsets", "earliest") \
                        .load()
                        
    kafka_in_value_df = raw_kafka_in_df.selectExpr("CAST(value AS STRING)")
    
    output_in_query = kafka_in_value_df.writeStream \
                          .queryName("Network") \
                          .format("memory") \
                          .start()
    output_in_query.awaitTermination(10)
     
    in_value_df = spark.sql("select * from Network")
     
    in_value_rdd = in_value_df.rdd.map(lambda i: i['value'].split(","))
    in_value_row_rdd = in_value_rdd.map(lambda i: Row(BusinessYear=i[0], \
                                                StateCode=i[1], \
                                                IssuerId=i[2], \
                                                SourceName=i[3], \
                                                VersionNum=i[4], \
                                                ImportDate=i[5], \
                                                IssuerId2=i[6], \
                                                StateCode2=i[7], \
                                                NetworkName=i[8], \
                                                NetworkId=i[9], \
                                                NetworkURL=i[10], \
                                                RowNumber=i[11], \
                                                MarketCoverage=i[12], \
                                                DentalOnlyPlan=i[13]))
    df_ins = spark.createDataFrame(in_value_row_rdd)
    df_ins.count()
    ins = df_ins.select('ImportDate').show()
    df_ins.printSchema()
    df_ins.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode('append') \
        .option('database','CDW_SAPP') \
        .option('collection', 'Network') \
        .option('uri', "mongodb://127.0.0.1/CDW_SAPP.Network") \
        .save()
def main():
    kafka_prod()
    spark_kafka()
main()    
print("Successfully loaded")