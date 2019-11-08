print('Producing messages.....')

import requests, os
from kafka import KafkaProducer
from pyspark.sql import SparkSession, Row
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
def kafka_prod():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/PlanAttributes.csv")

    
    data_list = [data for data in response.text.splitlines()[1:]]
    for data in data_list:

        producer.send('PlanAttributes', data.encode('utf-8'))
    producer.flush()
print('Consuming messages......')        
def spark_kafka():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

    spark = SparkSession.builder.getOrCreate()
    
    raw_kafka_in_df = spark.readStream \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", "localhost:9092") \
                        .option("subscribe", 'PlanAttributes') \
                        .option("startingOffsets", "earliest") \
                        .load()
                        
    kafka_in_value_df = raw_kafka_in_df.selectExpr("CAST(value AS STRING)")
    
    output_in_query = kafka_in_value_df.writeStream \
                          .queryName("PlanAttributes") \
                          .format("memory") \
                          .start()
    output_in_query.awaitTermination(10)
     
    in_value_df = spark.sql("select * from PlanAttributes")
     
    in_value_rdd = in_value_df.rdd.map(lambda i: i['value'].split("\t"))
    in_value_row_rdd = in_value_rdd.map(lambda i: Row(AttributesID=i[0], \
                                                BeginPrimaryCareCostSharingAfterNumberOfVisits=i[1], \
                                                BeginPrimaryCareDeductibleCoinsuranceAfterNumberOfCopays=i[2], \
                                                BenefitPackageId=i[3], \
                                                BusinessYear=i[4], \
                                                ChildOnlyOffering=i[5], \
                                                CompositeRatingOffered=i[6], \
                                                CSRVariationType=i[7], \
                                                DentalOnlyPlan=i[8], \
                                                DiseaseManagementProgramsOffered=i[9], \
                                                FirstTierUtilization=i[10], \
                                                HSAOrHRAEmployerContribution=i[11], \
                                                HSAOrHRAEmployerContributionAmount=i[12], \
                                                InpatientCopaymentMaximumDays=i[13], \
                                                IsGuaranteedRate=i[14], \
                                                IsHSAEligible=i[15], \
                                                IsNewPlan=i[16], \
                                                IsNoticeRequiredForPregnancy=i[17], \
                                                IsReferralRequiredForSpecialist=i[18], \
                                                IssuerId=i[19], \
                                                MarketCoverage=i[20], \
                                                MedicalDrugDeductiblesIntegrated=i[21], \
                                                MedicalDrugMaximumOutofPocketIntegrated=i[22], \
                                                MetalLevel=i[23], \
                                                MultipleInNetworkTiers=i[24], \
                                                NationalNetwork=i[25], \
                                                NetworkId=i[26], \
                                                OutOfCountryCoverage=i[27],   \
                                                OutOfServiceAreaCoverage=i[28], \
                                                PlanEffectiveDate=i[29], \
                                                PlanExpirationDate=i[30], \
                                                PlanId=i[31], \
                                                PlanLevelExclusions=i[32],  \
                                                PlanMarketingName=i[33], \
                                                PlanType=i[34], \
                                                QHPNonQHPTypeId=i[35],    \
                                                SecondTierUtilization=i[36],  \
                                                ServiceAreaId=i[37], \
                                                sourcename=i[38], \
                                                SpecialtyDrugMaximumCoinsurance=i[39], \
                                                StandardComponentId=i[40], \
                                                StateCode=i[41], \
                                                WellnessProgramOffered=i[42]))
    df_ins = spark.createDataFrame(in_value_row_rdd)
    df_ins.count()
    ins = df_ins.select('DentalOnlyPlan').show()
    df_ins.printSchema()
    df_ins.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode('append') \
        .option('database','CDW_SAPP') \
        .option('collection', 'PlanAttributes') \
        .option('uri', "mongodb://127.0.0.1/CDW_SAPP.PlanAttributes") \
        .save()
def main():
    kafka_prod()
    spark_kafka()
main() 
print("Successfully loaded")