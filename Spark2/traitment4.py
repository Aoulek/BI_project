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

# Filtrage des données pour exclure les articles nuls
df = df[df['Title'] != ""]

# Séparation de la colonne 'Categories' pour obtenir une liste de catégories
categories_df = df.withColumn("Categories", F.explode(F.split(df["Categories"], ";")))

# Filtrage des catégories vides
categories_df = categories_df[categories_df['Categories'] != ""]

# Collecte des catégories distinctes
distinct_categories = categories_df.select("Categories").distinct().collect()

# Affichage des catégories
categories_list = [row.Categories for row in distinct_categories]
print("Liste des catégories existantes :")
for category in categories_list:
    print(category)
