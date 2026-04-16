from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import logging

# Crear sesión

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema
schema = StructType([
    StructField("placa", StringType()),
    StructField("ciudad", StringType()),
    StructField("valor", IntegerType()),
    StructField("timestamp", TimestampType())
])

# Configurar lectura de straming
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reporte_multas") \
    .load()

# Parsear los datos JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Multas por ciudad (por ventana de 1 minuto)
multas_ciudad = parsed_df \
    .groupBy(window(col("timestamp"), "1 minute"), col("ciudad")) \
    .agg(count("*").alias("valor_total"))


# Mostrar resultados
query1 = multas_ciudad.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

spark.streams.awaitAnyTermination()




