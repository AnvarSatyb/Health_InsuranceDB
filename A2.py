print('Connecting to MariaDB......')

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

credit_df = spark.read.format('jdbc').options(
    url='jdbc:mysql://localhost:3306/cdw_sapp',
    driver = 'com.mysql.cj.jdbc.Driver',
    dbtable = 'cdw_sapp_creditcard',
    user = 'root').load()

print('Now fetching data from database....')

credit_df.createOrReplaceTempView('cdw_sapp_creditcard')
spark.sql('SELECT CREDIT_CARD_NO CUST_CC_NO,\
         CONCAT(YEAR, LPAD(Month, 2, 0), \
         LPAD(Day, 2, 0)) TIMEID, \
         CUST_SSN,\
         BRANCH_CODE,\
         TRANSACTION_TYPE,\
         TRANSACTION_VALUE,\
         TRANSACTION_ID \
 FROM cdw_sapp_creditcard')
credit_tr = spark.sql('SELECT CREDIT_CARD_NO CUST_CC_NO,\
         CONCAT(YEAR, LPAD(Month, 2, 0), \
         LPAD(Day, 2, 0)) TIMEID, \
         CUST_SSN,\
         BRANCH_CODE,\
         TRANSACTION_TYPE,\
         TRANSACTION_VALUE,\
         TRANSACTION_ID \
 FROM cdw_sapp_creditcard')
  
credit_tr.show()

print('Almost done....Loading data to MongoDB')

uri = "mongodb://127.0.0.1/CDW_SAPP.dbs"
spark_mongodb = SparkSession \
    .builder \
    .config('spark.mongodb.input.uri', uri) \
    .config('spark.mongodb.output.uri', uri) \
    .getOrCreate()


credit_tr.write \
    .format('com.mongodb.spark.sql.DefaultSource') \
    .mode('append') \
    .option('database','CDW_SAPP') \
    .option('collection','cdw_credit_collection') \
    .save()
    
print("Credit card database is loaded!!!")