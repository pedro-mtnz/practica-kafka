#!/usr/bin/env python

# Importo librerías necesarias

import pymongo
from kafka import KafkaConsumer
import ast
import logging
from datetime import datetime

log = logging.getLogger(__name__)

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
    print(f"Registro insertado en topic {record_metadata.topic}, con offset {record_metadata.offset}")


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
    print(f"No se ha podido establecer contacto con el cluster de Kafka: {error}")
    quit()

consumer.subscribe(['tweets_enriched'])

for message in consumer:
    mensaje = ast.literal_eval(message.value.decode('utf8'))
    # print(mensaje['tweet'])

    # Añado timestamp de inserción en DB
    mensaje["timetamp_insercion_db"] = str(datetime.now())

    insert_result = collection.insert_one(mensaje)
    print(f"Insertado tweet en base de datos: {mensaje['timetamp_insercion_db']}")

    tweets_collection = db.tweets
    count = tweets_collection.count_documents({})
    print(f"El número total de entradas en la colección 'tweets' es: {count}")

# Close the connection
client.close()
