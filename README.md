# analisis-multas-spark
Este proyecto tiene como objetivo el procesamiento y análisis de un conjunto de datos de multas de tránsito utilizando Apache Spark en un entorno Big Data en tiempo real

# objetivo

Desarrollar un flujo de datos en tiempo real que permita:

Procesar datos en streaming
Analizar el comportamiento de las multas en tiempo real

# Herrmientas utilizadas

Python 3
Apache Kafka
Apache Spark (PySpark)
Ubuntu 22.04

# Pasos para su ejecución

# 1. Iniciar servicio de apache.spark

start-master.sh 
sudo ss -tunelp | grep 8080 
start-slave.sh spark://osboxes:7077

# 2. Iniciar Kafka y Zookeeper

sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties & 

sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties & 

# 3. Creamos el topic
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic reporte_multas

# 4. Ejecutamos el productor 
python3 productor.py

nota: esto se ejecuta en una terminal y no se debe cerar para una buena ejecución

# 5. Ejecutar el consumidor (Spark Streaming)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming.py 

nota: esto se debe ejecutar en una terminal diferente al del paso numero 4

# Conclusión
Esto nos permite procesar datos en tiempo real, lo cual nos facilita un analisis mas rapido y dinamico 
lo cual es bastante beneficioso para la toma de decisiones donde la informacion se genera de forma muy rapida

# Autor
Carlos Vergara
