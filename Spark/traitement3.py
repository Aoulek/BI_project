# ----------------- Map : Nbre d'articles par pays ----------------- #
import pandas as pd
import pycountry as pycountry
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialisation de Spark
spark = SparkSession.builder.appName("SparkApp") \
    .master("local") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/bi_project.Articles") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/bi_project.Articles") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()
# Chargement des données depuis MongoDB
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://localhost:27017/bi_project.Articles").load()

# Filtrage des articles nuls et sans pays
df = df.filter(df['Title'] != "")
df = df.filter(df['Countries'] != "")

# Split des pays et comptage
x = df.select('Countries')
y = x.withColumn('Country', expr("explode(split(Countries, ';'))")).groupBy('Country').count().sort('count')

countries_number = y.toPandas()
print(countries_number)

# Supprimer les espaces au début des pays récupérés
countries_number['Country'] = countries_number['Country'].str.strip()

# Affichage des résultats
print(countries_number)

# Création du DataFrame final
countries_number_df = countries_number[['Country', 'count']].rename(columns={'count': 'Nbre Article'})

# Récupération des codes ISO pour les pays
countries_list = countries_number_df['Country'].unique().tolist()
countries_codes = {}

for country in countries_list:
    try:
        country_info = pycountry.countries.search_fuzzy(country)
        country_code = country_info[0].alpha_3
        countries_codes.update({country: country_code})
    except:
        print("Error: can't add this country's code => ", country)
        countries_codes.update({country: ' '})

countries_number_df['iso_alpha'] = countries_number_df['Country'].map(countries_codes)

# Création de la carte choroplèthe
fig = px.choropleth(
    data_frame=countries_number_df,
    locations='iso_alpha',
    color="Nbre Article",
    hover_name="Country",
    color_continuous_scale='RdYlGn'
)

# Affichage de la carte
fig.show()
