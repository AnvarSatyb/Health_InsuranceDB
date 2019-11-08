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
abc = data.groupby('region').count()['_id']
sm = data[data.smoker == 'yes'].groupby('region').count()['_id']
xyz = sm / abc
xyz.plot.bar(figsize=(10,10))
plt.show()