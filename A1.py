print('Connecting to MariaDB......')

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
  
branch_df = spark.read.format('jdbc').options(
    url='jdbc:mysql://localhost:3306/cdw_sapp',
    driver = 'com.mysql.cj.jdbc.Driver',
    dbtable = 'cdw_sapp_branch',
    user = 'root').load()

print('Now fetching data from database....')
    
branch_df.createOrReplaceTempView('cdw_sapp_branch')
spark.sql("SELECT BRANCH_CODE, BRANCH_NAME, BRANCH_STREET, BRANCH_CITY, BRANCH_STATE, \
IFNULL(branch_zip, 999999) BRANCH_ZIP, CONCAT('(', SUBSTRING(branch_phone,1,3),')', SUBSTRING(branch_phone, 4,3), '-', SUBSTRING(branch_phone,7)) BRANCH_PHONE, LAST_UPDATED \
FROM cdw_sapp_branch")
branch_tr = spark.sql("SELECT BRANCH_CODE, BRANCH_NAME, BRANCH_STREET, BRANCH_CITY, BRANCH_STATE, \
IFNULL(branch_zip, 999999) BRANCH_ZIP, CONCAT('(', SUBSTRING(branch_phone,1,3),')', SUBSTRING(branch_phone, 4,3), '-', SUBSTRING(branch_phone,7)) BRANCH_PHONE, LAST_UPDATED \
FROM cdw_sapp_branch")
branch_tr.show()

print('Almost done....Loading data to MongoDB')

uri = "mongodb://127.0.0.1/CDW_SAPP.dbs"
spark_mongodb = SparkSession \
    .builder \
    .config('spark.mongodb.input.uri', uri) \
    .config('spark.mongodb.output.uri', uri) \
    .getOrCreate()

branch_tr.write \
    .format('com.mongodb.spark.sql.DefaultSource') \
    .mode('append') \
    .option('database','CDW_SAPP') \
    .option('collection', 'cdw_branch_collection') \
    .save()

print("Table loaded successfully!!!")