#!/usr/bin/env python

# Importo librerías necesarias

import json
import logging
import random
import csv
import time
from datetime import datetime
from kafka import KafkaProducer

# Función de logging
def escribir_mensaje_log(message):
    archivo_log = "./log/Productor_tweets.log"
    with open(archivo_log, "a") as f:
        f.write(message + "\n")

log = logging.getLogger(__name__)

nombre_script = "Productor_tweets.py"

# Creamos nuestro Kafka Producer pasandole el broker desplegado
# Si el broker no está disponible se finaliza la ejecución
try:
    producer = KafkaProducer(
        bootstrap_servers=['127.0.0.1:9092'],
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )
except Exception as error:
    timestamp_actual = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    escribir_mensaje_log(f"{timestamp_actual} ({nombre_script}) No se ha podido establecer contacto con el cluster de Kafka: {error}")
    quit()

# Abro archivo de tweets descargado de Kaggle y lo guardo en una lista para acceder posteriormente a la columna de interés
file = open("/datos/Datahack/Módulo A2 - Kafka/Practica/datos/Tweets.csv", "r")
data = list(csv.reader(file, delimiter=","))
file.close()

# Funcion para el caso de exito en la produccion del metodo
def on_send_success(record_metadata):
    timestamp_actual = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    escribir_mensaje_log(f"{timestamp_actual} ({nombre_script}) Registro insertado en topic {record_metadata.topic}, en partición {record_metadata.partition} con offset {record_metadata.offset}")

# que haremos en caso de error
def on_send_error(ex):
    log.error('I am an Error', exc_info=ex)
    # handle exception

# Para simular que se revibe un número de tweets de forma aleatoria, se envía un número aletarior de tweets entre 10 y 100 y se espera 5 segundos para una nueva lectura
contador = 0
limite = random.randint(10, 100)
for tweet in data:
    if contador < limite:
        # En el topic inserto el tweet, su longitud y la hora de inserción
        # Este primer topic es 'topic_raw'
        mensaje = {
            'tweet': tweet[1],
            'longitud': len(tweet[1]),
            'timestamp_tweet': str(datetime.now())
        }
        key = str(random.randint(0, 9))
        producer.send('tweets_raw', key=key.encode('utf-8'), value=mensaje).add_callback(on_send_success).add_errback(on_send_error)
        contador += 1
    else:
        contador = 0
        limite = random.randint(10, 100)
        time.sleep(5)

producer.flush()