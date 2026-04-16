import time
import json
import random
import string
from kafka import KafkaProducer

# Generador de datos
def generar_multa():
    ciudades = ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena"]

    placa = ''.join(random.choices(string.ascii_uppercase, k=3)) + \
            ''.join(random.choices(string.digits, k=3))

    return {
        "placa": placa,
        "ciudad": random.choice(ciudades),
        "valor": random.randint(100000, 1000000),
        "timestamp": int(time.time())
    }

# Productor Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Envío continuo
while True:
    data = generar_multa()
    producer.send('reporte_multas', value=data)
    print("Enviado:", data)

    time.sleep(1)

