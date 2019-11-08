print('Connecting to MongoDB database...')

import pymongo
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
client = MongoClient()
print('Loading Image......')
db = client.CDW_SAPP
collection = db.ServiceArea
data = pd.DataFrame(list(collection.find()))
state_count_df = data.groupby('StateCode').count()
abc = state_count_df[['ServiceAreaName','SourceName','BusinessYear']]
abc.plot.bar(figsize=(10,10))
plt.show()