from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, udf, current_timestamp, 
    to_date, length, to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType
import re
from prometheus_client import start_http_server, Gauge

SCRIPT_STATUS = Gauge('news_script_status', 'Statut du script (1=Running)')
ARTICLES_BRONZE_TOTAL = Gauge('news_articles_bronze_total', 'Total articles traités en Bronze')
ARTICLES_SILVER_TOTAL = Gauge('news_articles_silver_total', 'Total articles traités en Silver')

try:
    start_http_server(8002, addr='0.0.0.0')
    print("MÉTRIQUES : Serveur lancé sur 0.0.0.0:8002")
except Exception as e:
    print(f"MÉTRIQUES : Erreur au lancement : {e}")
SCRIPT_STATUS.set(1)

bronze_counter = 0
silver_counter = 0

def update_bronze_metrics(batch_df, batch_id):
    global bronze_counter
    batch_count = batch_df.count()
    if batch_count > 0:
        bronze_counter += batch_count
        ARTICLES_BRONZE_TOTAL.set(bronze_counter)
        print(f">>> MÉTRIQUES : Bronze mis à jour : {bronze_counter}")

def update_silver_metrics(batch_df, batch_id):
    global silver_counter
    batch_count = batch_df.count()
    if batch_count > 0:
        silver_counter += batch_count
        ARTICLES_SILVER_TOTAL.set(silver_counter)
        print(f">>> MÉTRIQUES : Silver mis à jour : {silver_counter}")

# ====================================
# CONFIGURATION SPARK
# ====================================
spark = SparkSession.builder \
    .appName("News_Medallion_Bronze_Silver") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ====================================
# SCHEMA
# ====================================
schema = StructType([
    StructField("titre", StringType(), False),
    StructField("auteur", StringType(), True),
    StructField("date_publication", StringType(), True),
    StructField("categorie", StringType(), True),
    StructField("contenu", StringType(), True),
    StructField("source", StringType(), False),
    StructField("url", StringType(), False)
])

# ====================================
# LECTURE KAFKA EN STREAMING
# ====================================
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "news_articles") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()


# Parsing initial
parsed_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp") \
 .withColumn("ingestion_time", current_timestamp())

query_metrics_bronze = parsed_df.writeStream \
    .foreachBatch(update_bronze_metrics) \
    .trigger(processingTime="1 minute") \
    .start()

# ====================================
# COUCHE BRONZE (SAUVEGARDE BRUTE JSON)
# ====================================
query_bronze = parsed_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "s3a://bronze/articles/") \
    .option("checkpointLocation", "s3a://bronze/checkpoints_v2/") \
    .trigger(processingTime="1 minute") \
    .start()

# ====================================
# COUCHE SILVER (NETTOYAGE ET SAUVEGARDE PARQUET)
# ====================================
# Fonction de nettoyage (UDF)
def clean_text(text):
    if not text: return ""
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'[^\w\s\u00C0-\u017F.,!?\-\'"]', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()

clean_udf = udf(clean_text, StringType())

# Transformations Silver
silver_df = parsed_df \
    .withWatermark("ingestion_time", "1 hour") \
    .filter(col("titre").isNotNull() & col("url").isNotNull()) \
    .withColumn("contenu_clean", clean_udf(col("contenu"))) \
    .withColumn("titre_clean", clean_udf(col("titre"))) \
    .withColumn("date_parsed", to_date(to_timestamp(col("date_publication")))) \
    .withColumn("auteur_clean", clean_udf(col("auteur"))) \
    .withColumn("categorie_clean", clean_udf(col("categorie"))) \
    .dropDuplicates(["url"]) \
    .select(
        "url", "titre_clean", "auteur_clean", 
        "categorie_clean", "date_parsed", "source", 
        "contenu_clean", "ingestion_time"
    )

query_metrics_silver = silver_df.writeStream \
    .foreachBatch(update_silver_metrics) \
    .trigger(processingTime="1 minute") \
    .start()

query_silver = silver_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://silver/articles_clean/") \
    .option("checkpointLocation", "s3a://silver/checkpoints_v2/") \
    .option("compression", "snappy") \
    .trigger(processingTime="1 minute") \
    .start()

# Maintient le script en vie indéfiniment
spark.streams.awaitAnyTermination()
