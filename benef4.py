print('Producing messages.....')


import requests, os
from kafka import KafkaProducer
from pyspark.sql import SparkSession, Row
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
def kafka_prod():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partFour.txt")
  
      
    data_list = [data for data in response.text.splitlines()[1:]]
    for data in data_list:
        producer.send('benefit_4', data.encode('utf-8'))
    producer.flush()
print('Consuming messages......')         
def spark_kafka():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

    spark = SparkSession.builder.getOrCreate()
      
    raw_kafka_in_df = spark.readStream \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", "localhost:9092") \
                        .option("subscribe", 'benefit_4') \
                        .option("startingOffsets", "earliest") \
                        .load()
                          
    kafka_in_value_df = raw_kafka_in_df.selectExpr("CAST(value AS STRING)")
      
    output_in_query = kafka_in_value_df.writeStream \
                          .queryName("benef4345346") \
                          .format("memory") \
                          .start()
    output_in_query.awaitTermination(10)
       
    in_value_df = spark.sql("select * from benef4345346")
       
    in_value_rdd = in_value_df.rdd.map(lambda i: i['value'].split("\t"))
    in_value_row_rdd = in_value_rdd.map(lambda i: Row(BenefitName=i[0], \
                                                BusinessYear=i[1], \
                                                EHBVarReason=i[2], \
                                                IsCovered=i[3], \
                                                IssuerId=i[4], \
                                                LimitQty=i[5], \
                                                LimitUnit=i[6], \
                                                MinimumStay=i[7], \
                                                PlanId=i[8], \
                                                SourceName=i[9], \
                                                StateCod=i[10]))
    df_ins = spark.createDataFrame(in_value_row_rdd)
    df_ins.count()
    ins = df_ins.select('PlanId').show()
    df_ins.printSchema()
    df_ins.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode('append') \
        .option('database','CDW_SAPP') \
        .option('collection', 'benefit') \
        .option('uri', "mongodb://127.0.0.1/CDW_SAPP.benefit") \
        .save()
def main():
     kafka_prod()
     spark_kafka()
main()   
print('Benefit part 4 data is successfully loaded to MongoDB') 