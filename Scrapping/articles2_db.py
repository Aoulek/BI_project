import json
from pymongo import MongoClient

client = MongoClient('localhost', 27017)

# le nom de bdd
db = client['bi_project']

# le nom de collection
collection = db['Articles_Q']
# collection = db['scopus']

# le chemin de data sous form json

with open('C:/Users/pc/Desktop/BigData-Bi-Project/projet/articles_Q.json') as file:
    file_data = json.load(file)

collection.insert_many(file_data)

client.close()
