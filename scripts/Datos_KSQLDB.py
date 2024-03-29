#!/usr/bin/env python

# Importo librerías necesarias

import requests
import json
import re
import time
from datetime import datetime

# Función de logging
def escribir_mensaje_log(message):
    archivo_log = "./log/Datos_KSQLDB.log"
    with open(archivo_log, "a") as f:
        f.write(message + "\n")

nombre_script = "Datos_KSQLDB.py"

# Preparo los datos para la conexión con KSQLDB
url = "http://localhost:8088/query"
headers = {
    "Content-Type": "application/vnd.ksql.v1+json"
}
payload = {
    "ksql": "SELECT * FROM tabla_sentimientos;"
}

# payload to JSON
json_payload = json.dumps(payload)

# Genero la página web en la que voy a escribir los datos:

primera_parte = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset=\"UTF-8\">
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0">
    <title>Práctica Kafka - Pedro Martínez</title>
</head>
<body>

<h1>Estadísticas de análisis de sentimientos</h1>

<p>En esta página se muestran los valores actuales:</p>

"""

# Lo dejo corriendo en bucle infinito generando los datos en la web
while True:
    response = requests.post(url, headers=headers, data=json_payload)

    # Expresión regular para capturar los valores de lo que devuelve KSQLDB
    pattern = r'\["([^"]+)",(\d+)\]'

    if response.status_code == 200:
        timestamp_actual = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        matches = re.findall(pattern, response.text)
        result_dict = {sentiment: int(count) for sentiment, count in matches}

        if not 'positivo' in result_dict or not 'negativo' in result_dict or not 'neutro' in result_dict:
            continue
            
        total = result_dict['negativo'] + result_dict['positivo'] + result_dict['neutro']
        
        porcentaje_positivo = result_dict['positivo'] * 100 / total
        porcentaje_negativo = result_dict['negativo'] * 100 / total
        porcentaje_neutro = result_dict['neutro'] * 100 / total
        segunda_parte = f"""
            <h2>Hora actual: {timestamp_actual}</h2>
            <ul>
                <li>Comentarios negativos: {result_dict['negativo']} ({round(porcentaje_negativo, 2)} %)</li>
                <li>Comentarios positivos: {result_dict['positivo']} ({round(porcentaje_positivo, 2)} %)</li>
                <li>Comentarios neutros: {result_dict['neutro']} ({round(porcentaje_neutro, 2)} %)</li>
            </ul>

            </body>
            </html>
        """
        # Escribo el archivo de salida
        with open('./web/estadisticas_sentimientos.html', 'w') as file:
            print(f"{primera_parte}\n{segunda_parte}", file=file)

        escribir_mensaje_log(f"{timestamp_actual} ({nombre_script}) Se ha generado una nueva página web con datos actualizados")

    else:
        # Si el código no es 200, imprimo el error
        print("Error:", response.text)

    # Espero 5 segundos para volve a refrescar los datos
    time.sleep(5)
