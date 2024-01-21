import json
from pymongo import MongoClient

client = MongoClient('localhost', 27017)


db = client['bi_project']


collection = db['Articles']



with open('C:/Users/pc/Desktop/BigData-Bi-Project/projet/Articles_final.json') as file:
    
    file_data = json.load(file)

collection.insert_many(file_data)

client.close()
