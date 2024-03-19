#!/bin/bash

# Configuraciones

## Color mensajes de log
color_code="32"
## Ruta base desde donde ejecutar la práctica
PATH_BASE="/datos/Datahack/Módulo A2 - Kafka/Practica/"
## Ruta donde leer docker-compose file
DOCKER_COMPOSE_FILE="$PATH_BASE/despliegue/docker-compose.yml"
## Ruta donde leer requirementes para librerías Python
PIP_REQUIREMENTS="$PATH_BASE/despliegue/requirements.txt"
## Ruta scripts Python 
PRODUCTOR_TWEETS="$PATH_BASE/scripts/Productor_tweets.py"
ANALISIS_SENTIMIENTOS_1="$PATH_BASE/scripts/Analizador_sentimientos-1.py"
ANALISIS_SENTIMIENTOS_2="$PATH_BASE/scripts/Analizador_sentimientos-2.py"
ANALISIS_SENTIMIENTOS_3="$PATH_BASE/scripts/Analizador_sentimientos-3.py"
DATOS_KSQLDB="$PATH_BASE/scripts/Datos_KSQLDB.py"
DATOS_HACIA_MONGODB="$PATH_BASE/scripts/Datos_hacia_MongoDB.py"

# Display a message indicating the Docker Compose process has started
echo ""
echo -e "\e[${color_code}m$(date +'%Y-%m-%d %H:%M:%S'): Lanzo docker-compose\e[0m"
echo ""

# Start Docker Compose in detached mode and log output
docker-compose -f "$DOCKER_COMPOSE_FILE" up -d

# Display a message indicating the Docker Compose process has started
echo ""
echo -e "\e[${color_code}m$(date +'%Y-%m-%d %H:%M:%S'): Se finalzia la ejecución de docker-compose\e[0m"
echo ""

# Display a message indicating the Docker Compose process has started
echo ""
echo -e "\e[${color_code}m$(date +'%Y-%m-%d %H:%M:%S'): Instalación de librerías de Python necesarias usando archivo $PIP_REQUIREMENTS\e[0m"
echo ""

pip install -r "$PIP_REQUIREMENTS"

# Display a message indicating the Docker Compose process has started
echo ""
echo -e "\e[${color_code}m$(date +'%Y-%m-%d %H:%M:%S'): Fin de la instalación de librerías de Python necesarias\e[0m"

# Creo los topics

echo ""
echo -e "\e[${color_code}m$(date +'%Y-%m-%d %H:%M:%S'): Creo los topics necesarios para la práctica\e[0m"
echo ""

docker exec -it broker kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic tweets_enriched
docker exec -it broker kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic tweets_raw

# Listo los topics que contienen la palabra 'tweets'

echo ""
echo -e "\e[${color_code}m$(date +'%Y-%m-%d %H:%M:%S'): Los topics que comienzan por 'tweets' son:\e[0m"
docker exec -it broker kafka-topics --list --bootstrap-server localhost:9092 | grep tweets
echo ""

# Creo STREAM y TABLE en KSQLDB

echo ""
echo -e "\e[${color_code}m$(date +'%Y-%m-%d %H:%M:%S'): Espero 30 segundos, para dar tiempo a estar disponible a KSQLDB-SERVER:\e[0m"
echo ""
sleep 30

echo ""
echo -e "\e[${color_code}m$(date +'%Y-%m-%d %H:%M:%S'): Creo STREAM en KSQLDB:\e[0m"
echo ""

docker exec -i ksqldb-cli ksql http://ksqldb-server:8088 <<EOF
CREATE STREAM tweets (tweet STRING, sentimiento_detectado STRING, longitud INT, polaridad DOUBLE, timestamp_tweet STRING, timestamp_enriquecimiento STRING) WITH (KAFKA_TOPIC='tweets_enriched', VALUE_FORMAT='JSON');
EOF

echo ""
echo -e "\e[${color_code}m$(date +'%Y-%m-%d %H:%M:%S'): Muestro los STREAMS creados:\e[0m"
echo ""

docker exec -i ksqldb-cli ksql http://ksqldb-server:8088 <<EOF
SHOW STREAMS;
EOF

# echo '{"tweet": "AAA", "sentimiento_detectado": "neutro"}' | docker exec -i broker kafka-console-producer --bootstrap-server localhost:9092 --topic tweets_enriched

echo ""
echo -e "\e[${color_code}m$(date +'%Y-%m-%d %H:%M:%S'): Creo TABLE en KSQLDB:\e[0m"
echo ""

docker exec -i ksqldb-cli ksql http://ksqldb-server:8088 <<EOF
CREATE TABLE tabla_sentimientos AS SELECT sentimiento_detectado, count(*) FROM tweets GROUP BY sentimiento_detectado EMIT CHANGES;
EOF

echo ""
echo -e "\e[${color_code}m$(date +'%Y-%m-%d %H:%M:%S'): Muestro las TABLES creados:\e[0m"
echo ""

docker exec -i ksqldb-cli ksql http://ksqldb-server:8088 <<EOF
SHOW TABLES;
EOF

# Lanzo Producer de Tweets

echo ""
echo -e "\e[${color_code}m$(date +'%Y-%m-%d %H:%M:%S'): Lanzo Producer - Lee Tweets e inserta en topic:\e[0m"
echo ""

# python3 "$PRODUCTOR_TWEETS" &
# python3 "$ANALISIS_SENTIMIENTOS_1" &
# python3 "$ANALISIS_SENTIMIENTOS_2" &
# python3 "$ANALISIS_SENTIMIENTOS_3" &
# python3 "$DATOS_KSQLDB" &
# python3 "$DATOS_HACIA_MONGODB" &