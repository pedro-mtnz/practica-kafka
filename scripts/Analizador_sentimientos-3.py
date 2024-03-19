#!/usr/bin/env python

import json

from kafka import KafkaConsumer, KafkaProducer
from textblob import TextBlob
import ast
import logging
import random
from datetime import datetime

log = logging.getLogger(__name__)

# Funcion para el caso de exito en la produccion del metodo
def on_send_success(record_metadata):
  print(f"Registro insertado en topic {record_metadata.topic}, con offset {record_metadata.offset}")

# que haremos en caso de error
def on_send_error(ex):
  log.error('I am an Error', exc_info=ex)
  # handle exception

try:
  consumer = KafkaConsumer('tweets_raw',
    group_id='consumer-group-1',
    bootstrap_servers='127.0.0.1:9092',
    auto_offset_reset='earliest'
  )

  producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
  )
  
except Exception as error:
  print(f"No se ha podido establecer contacto con el cluster de Kafka: {error}")
  quit()

consumer.subscribe(['tweets_raw'])

for message in consumer:
  # Leo cada mensaje del topic y lo guardo como diccionario
  # Hago el análisis de sentimiento y lo inserto como nuevo campo en el diccionario
  # Añado tanto el análisis como el valor del polarity obtenido
  # Añado también un nuevo timestamp para indicar el momento del enriquecimiento
  # El resultado se inserta en el topic tweets_enriched
  mensaje = ast.literal_eval(message.value.decode('utf8'))
  analysis = TextBlob(mensaje['tweet'])
  sentimiento = "neutro"
  if analysis.sentiment.polarity > 0:
      sentimiento = "positivo"
  elif analysis.sentiment.polarity < 0:
      sentimiento = "negativo"
  mensaje['sentimiento_detectado'] = sentimiento
  mensaje['polaridad'] = analysis.sentiment.polarity
  mensaje['timestamp_enriquecimiento'] = str(datetime.now())
  print(mensaje)
  key = str(random.randint(0, 9))
  producer.send('tweets_enriched', key=key.encode('utf-8'), value=mensaje).add_callback(on_send_success).add_errback(on_send_error)

producer.flush()