print('Connecting to MongoDB database...')
import pymongo
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
client = MongoClient()
print('Loading Image......')
db = client.CDW_SAPP
collection = db.benefit
data = pd.DataFrame(list(collection.find()))
d3 = data[['BenefitName','StateCod']].groupby(['StateCod'])['BenefitName'].count()
d3.plot.bar(figsize = (10,10))
plt.show()