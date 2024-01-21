import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Configuration de la session Spark
spark = SparkSession.builder.appName("SparkApp") \
    .master("local") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/bi_project.Articles_Q") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/bi_project.Articles_Q") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Chargement des données depuis MongoDB
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://localhost:27017/bi_project.Articles_Q").load()

# Filtrage des données
df = df[df['Title'] != ""]  # Eliminer les articles nuls

# Groupement par 'SJR Best Quartile' et comptage
gr = df.groupBy("SJR Best Quartile").count().sort("SJR Best Quartile")

# Collecter les résultats en pandas
gr_pandas = gr.toPandas()

# Création du graphique à barres
plt.bar(gr_pandas['SJR Best Quartile'], gr_pandas['count'])
plt.title("Nombre d'articles par SJR Best Quartile")
plt.xlabel("SJR Best Quartile")
plt.ylabel("Nombre d'articles")
plt.show()
