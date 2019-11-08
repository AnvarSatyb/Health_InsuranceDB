print('Connecting to MariaDB......')
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

customer_df = spark.read.format('jdbc').options(
    url='jdbc:mysql://localhost:3306/cdw_sapp',
    driver = 'com.mysql.cj.jdbc.Driver',
    dbtable = 'cdw_sapp_customer',
    user = 'root').load()
print('Now fetching data from database....')   
customer_df.createOrReplaceTempView('cdw_sapp_customer')
spark.sql("SELECT CAST(SSN AS INT) CUST_SSN, \
CONCAT(UCASE(SUBSTRING(FIRST_NAME,1,1)), LCASE(SUBSTRING(FIRST_NAME,2))) CUST_F_NAME, \
CONCAT(UCASE(SUBSTRING(last_name,1,1)), LCASE(SUBSTRING(last_name,2))) CUST_L_NAME, \
Credit_card_no CUST_CC_NO, \
CONCAT(STREET_NAME,' ',APT_NO) CUST_STREET, \
CUST_CITY, CUST_STATE, CUST_COUNTRY, CUST_ZIP, CONCAT(SUBSTRING(cust_phone, 1,3), '-',SUBSTRING(cust_phone,4)) CUST_PHONE, LOWER(MIDDLE_NAME) CUST_M_NAME, CUST_EMAIL, LAST_UPDATED FROM cdw_sapp_customer")
customer_tr = spark.sql("SELECT CAST(SSN AS INT) CUST_SSN, \
CONCAT(UCASE(SUBSTRING(FIRST_NAME,1,1)), LCASE(SUBSTRING(FIRST_NAME,2))) CUST_F_NAME, \
CONCAT(UCASE(SUBSTRING(last_name,1,1)), LCASE(SUBSTRING(last_name,2))) CUST_L_NAME, \
Credit_card_no CUST_CC_NO, \
CONCAT(STREET_NAME,APT_NO) CUST_STREET, \
CUST_CITY, CUST_STATE, CUST_COUNTRY, CUST_ZIP, CONCAT(SUBSTRING(cust_phone, 1,3), '-',SUBSTRING(cust_phone,4)) CUST_PHONE, LOWER(MIDDLE_NAME) CUST_M_NAME, CUST_EMAIL, LAST_UPDATED FROM cdw_sapp_customer")

customer_tr.show() 

print('Almost done....Loading data to MongoDB')

uri = "mongodb://127.0.0.1/CDW_SAPP.dbs"
spark_mongodb = SparkSession \
    .builder \
    .config('spark.mongodb.input.uri', uri) \
    .config('spark.mongodb.output.uri', uri) \
    .getOrCreate()
    
customer_tr.write \
    .format('com.mongodb.spark.sql.DefaultSource') \
    .mode('append') \
    .option('database','CDW_SAPP') \
    .option('collection','cdw_customer_collection') \
    .save()
    
print("Customer database is loaded")