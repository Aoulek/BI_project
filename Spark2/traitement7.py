import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("SparkApp") \
    .master("local") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/bi_project.Articles_Q") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/bi_project.Articles_Q") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Chargement des données depuis MongoDB
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://localhost:27017/bi_project.Articles_Q").load()

# Filtrage des données pour exclure les articles nuls
df = df[df['Title'] != ""]

# Séparation de la colonne 'Categories' pour obtenir une liste de catégories
categories_df = df.withColumn("Categories", F.explode(F.split(df["Categories"], ";")))

# Filtrage des catégories vides
categories_df = categories_df[categories_df['Categories'] != ""]

# Conversion de la colonne 'Year' en entier
categories_df = categories_df.withColumn("Year", categories_df["Year"].cast("int"))

# Groupement par catégorie, année et comptage
gr = categories_df.groupBy("Categories", "Year").count().sort("Year")

# Collecter les résultats en pandas
gr_pandas = gr.toPandas()

# Utilisation de la méthode plot de Pandas pour le graphique à barres
gr_pandas.pivot(index='Year', columns='Categories', values='count').plot(kind='bar', stacked=True)
plt.title("Nombre d'articles par catégorie et par année")
plt.xlabel("Année")
plt.ylabel("Nombre d'articles")
plt.legend(title="Catégorie", bbox_to_anchor=(1, 1))
plt.show()
