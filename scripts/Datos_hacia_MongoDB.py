#!/usr/bin/env python

# Importo librerías necesarias

import pymongo
from kafka import KafkaConsumer
import ast
import logging
from datetime import datetime

# Función de logging
def escribir_mensaje_log(message):
    archivo_log = "./log/Datos_hacia-MongoDB-1.log"
    with open(archivo_log, "a") as f:
        f.write(message + "\n")

log = logging.getLogger(__name__)

nombre_script = "Datos_hacia_MongoDB.py"

# Datos de conexión con MongoDB
mongo_host = "localhost"
mongo_port = 27017
mongo_username = "admin"
mongo_password = "admin"
mongo_auth_source = "admin"  # Replace with the appropriate authentication database

# Connection string con authenticacion
connection_string = f"mongodb://{mongo_username}:{mongo_password}@{mongo_host}:{mongo_port}/{mongo_auth_source}"

# Connect to MongoDB
client = pymongo.MongoClient(connection_string)

# Access your database and collection
db = client["mydatabase"]
collection = db["tweets"]

# Funcion para el caso de exito en la produccion del metodo
def on_send_success(record_metadata):
    timestamp_actual = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    escribir_mensaje_log(f"{timestamp_actual} ({nombre_script}) Registro insertado en topic {record_metadata.topic}, con offset {record_metadata.offset}")


# que haremos en caso de error
def on_send_error(ex):
    log.error('I am an Error', exc_info=ex)
    # handle exception

try:
  consumer = KafkaConsumer('tweets_enriched',
    group_id='py-group',
    bootstrap_servers='127.0.0.1:9092',
    auto_offset_reset='earliest')

except Exception as error:
    timestamp_actual = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    escribir_mensaje_log(f"{timestamp_actual} ({nombre_script}) No se ha podido establecer contacto con el cluster de Kafka: {error}")
    quit()

consumer.subscribe(['tweets_enriched'])

for message in consumer:
    timestamp_actual = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    mensaje = ast.literal_eval(message.value.decode('utf8'))
    # print(mensaje['tweet'])

    # Añado timestamp de inserción en DB
    mensaje["timetamp_insercion_db"] = str(datetime.now())

    insert_result = collection.insert_one(mensaje)
    escribir_mensaje_log(f"{timestamp_actual} ({nombre_script}) Insertado tweet en base de datos: {mensaje['timetamp_insercion_db']}")

    tweets_collection = db.tweets
    count = tweets_collection.count_documents({})
    escribir_mensaje_log(f"{timestamp_actual} ({nombre_script}) El número total de entradas en la colección 'tweets' es: {count}")

# Close the connection
client.close()
