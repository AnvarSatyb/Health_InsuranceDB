print('Connecting to MongoDB database...')
import pymongo
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
client = MongoClient()
print('Loading Image......')
db = client.CDW_SAPP
collection = db.insurance
data = pd.DataFrame(list(collection.find()))            
print('Smoking mothers quantity in this database is', data[(data['sex']=='female')& (data['smoker']=='yes')& (data['children']>0)]['_id'].count())
