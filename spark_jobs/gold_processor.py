from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    desc,
    current_timestamp,
    to_date
)

# ==========================================
# SPARK SESSION
# ==========================================

spark = SparkSession.builder \
    .appName("GoldLayerProcessor") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# LECTURE SILVER
# ==========================================

try:
    silver_df = spark.read.parquet(
        "s3a://silver/articles_clean/"
    )
    print("Lecture SILVER terminée")
except Exception as e:
    print("Erreur lors de la lecture de la couche SILVER :", str(e))
    spark.stop()
    exit(1)

# ==========================================
# ARTICLES PAR JOUR
# ==========================================

articles_par_jour = silver_df \
    .groupBy("date_parsed") \
    .agg(
        count("*").alias("nombre_articles")
    ) \
    .withColumn("processing_time", current_timestamp())

# ==========================================
# ARTICLES PAR SOURCE
# ==========================================

articles_par_source = silver_df \
    .groupBy("source") \
    .agg(
        count("*").alias("nombre_articles")
    ) \
    .withColumn("processing_time", current_timestamp())

# ==========================================
# TOP CATEGORIES
# ==========================================

top_categories = silver_df \
    .groupBy("categorie_clean") \
    .agg(
        count("*").alias("total")
    ) \
    .orderBy(desc("total")) \
    .withColumn("processing_time", current_timestamp())

# ==========================================
# ARTICLES PAR DATE + SOURCE
# ==========================================

articles_par_source_jour = silver_df \
    .groupBy(
        "source",
        "date_parsed"
    ) \
    .agg(
        count("*").alias("nombre_articles")
    ) \
    .withColumn("processing_time", current_timestamp())

# ==========================================
# JDBC CONFIG
# ==========================================

jdbc_url = "jdbc:postgresql://postgres:5432/warehouse"

jdbc_properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# ==========================================
# WRITE TABLES
# ==========================================

articles_par_jour.write \
    .mode("overwrite") \
    .jdbc(
        url=jdbc_url,
        table="articles_par_jour",
        properties=jdbc_properties
    )

print("Table articles_par_jour créée")

articles_par_source.write \
    .mode("overwrite") \
    .jdbc(
        url=jdbc_url,
        table="articles_par_source",
        properties=jdbc_properties
    )

print("Table articles_par_source créée")

top_categories.write \
    .mode("overwrite") \
    .jdbc(
        url=jdbc_url,
        table="top_categories",
        properties=jdbc_properties
    )

print("Table top_categories créée")

articles_par_source_jour.write \
    .mode("overwrite") \
    .jdbc(
        url=jdbc_url,
        table="articles_par_source_jour",
        properties=jdbc_properties
    )

print("Table articles_par_source_jour créée")

# ==========================================
# FIN
# ==========================================

print("\n==============================")
print("GOLD LAYER TERMINÉ")
print("==============================")

spark.stop()