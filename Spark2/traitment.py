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

# Séparation de la colonne 'Categories' pour obtenir une liste de catégories
categories_df = df.withColumn("Categories", F.explode(F.split(df["Categories"], ";")))

# Filtrage des catégories vides
categories_df = categories_df[categories_df['Categories'] != ""]

# Filtrage des données pour trois catégories spécifiques
target_categories = ["Marketing (Q1)", "Computer Networks and Communications (Q1)", "Information Systems (Q1)"]
selected_categories_df = categories_df[categories_df['Categories'].isin(target_categories)]

# Groupement par catégorie et comptage
gr = selected_categories_df.groupBy("Categories").count().sort("count", ascending=False)

# Collecter les résultats en pandas
gr_pandas = gr.toPandas()

# Création du graphique à barres
plt.bar(gr_pandas['Categories'], gr_pandas['count'])
plt.title("Nombre d'articles pour les catégories spécifiques")
plt.xlabel("Catégorie")
plt.ylabel("Nombre d'articles")
plt.xticks(rotation=45, ha="right")
plt.show()