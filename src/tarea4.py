# Importar librerías necesarias
from pyspark.sql import SparkSession, functions as F
import time

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Analisis_Multas').getOrCreate()

# Cargar datos desde HDFS
file_path = 'hdfs://localhost:9000/Tarea3/72nf-y4v3.csv'

# Leer el archivo
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

# Imprimimos el esquema
df.printSchema()

# Imprimimos los primeros 5 datos
print(df.columns)
df.show(5)

# Limpieza de los datos null y nan
df = df.dropna()

# Valor total de multas
print("=== VALOR TOTAL DE MULTAS ===")
df.select(F.sum("valor_multa")).show()

# Ciudad con más multas
print("=== CIUDAD CON MÁS MULTAS ===")

df.groupBy("ciudad") \
    .count() \
    .withColumnRenamed("count", "numero_multas") \
    .orderBy(F.desc("numero_multas")) \
    .show()

#  Placas repetidas y ciudad
print("=== PLACAS REPETIDAS ===")

df.groupBy("placa", "ciudad") \
    .count() \
    .withColumnRenamed("count", "veces_repetido") \
    .filter(F.col("veces_repetido") > 1) \
    .orderBy(F.desc("veces_repetido")) \
    .show()

# Porcentaje de multas por ciudad
print("=== PORCENTAJE DE MULTAS POR CIUDAD ===")

total = df.count()

df.groupBy("ciudad") \
    .count() \
    .withColumnRenamed("count","numero_multas") \
    .withColumn("porcentaje", F.round((F.col("numero_multas") / total) * 100)) \
    .orderBy(F.desc("porcentaje")) \
    .show()

time.sleep(10000)
spark.stop()

