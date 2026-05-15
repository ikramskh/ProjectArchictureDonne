from pyspark.sql import SparkSession

# 1. Initialisation de Spark (avec les mêmes configs MinIO)
spark = SparkSession.builder \
    .appName("Verify_Silver_Layer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=== LECTURE DE LA COUCHE SILVER ===")

try:
    # 2. Lecture du dossier Parquet
    silver_df = spark.read.parquet("s3a://silver/articles_clean/")

    # 3. Vérifications
    print("\n--- SCHÉMA DES DONNÉES ---")
    silver_df.printSchema()

    print(f"\n--- NOMBRE TOTAL D'ARTICLES : {silver_df.count()} ---")

    print("\n--- APERÇU DES 5 DERNIERS ARTICLES ---")
    # On trie par date d'ingestion décroissante pour voir les plus récents
    silver_df.orderBy(silver_df.ingestion_time.desc()).select(
        "source", "titre_clean", "auteur_clean", "date_parsed"
    ).show(5, truncate=False)

except Exception as e:
    print(f"Erreur lors de la lecture (le dossier est peut-être vide) : {e}")

spark.stop()