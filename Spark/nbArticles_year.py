import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Configuration de la session Spark
spark = SparkSession.builder.appName("SparkApp") \
    .master("local") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/bi_project.Articles") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/bi_project.Articles") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Chargement des données depuis MongoDB
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://localhost:27017/bi_project.Articles").load()

# Filtrage des données
df = df[df['Title'] != ""]  # Eliminer les articles nuls
df = df[df['Year'] != ""]   # Eliminer les articles ayant une année nulle

# Conversion de la colonne 'Year' en entier
df = df.withColumn("Year", df["Year"].cast("int"))

# Groupement par année et comptage
gr = df.groupBy("Year").count().sort("Year")

# Collecter les résultats en pandas
gr_pandas = gr.toPandas()

# Création du graphique à barres
plt.bar(gr_pandas['Year'], gr_pandas['count'])
plt.title("Nombre d'articles Blockchain par année")
plt.xlabel("Année")
plt.ylabel("Nombre d'articles Blockchain")
plt.show()
