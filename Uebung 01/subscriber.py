import paho.mqtt.client as mqtt
import psycopg2
import os
import uuid
import json

from dotenv import load_dotenv

# Lädt die Umgebungsvariablen aus der .env-Datei
load_dotenv()

# Zugriff auf die Umgebungsvariablen
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

# Verbindung zur Datenbank herstellen
def connect_db():
    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password
    )
    return conn

# Funktion zum Einfügen der Nachricht in die Datenbank
def insert_into_database(message):
    conn = connect_db()
    cur = conn.cursor()

    cur.execute("INSERT INTO staging.messung (payload) VALUES (%s)", (message,))
    conn.commit()

    cur.close()
    conn.close()

def on_message(client, userdata, message):
    message = message.payload.decode()
    #message = json.dumps(message)
    print(message)
    insert_into_database(message)

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, uuid.uuid4().hex, clean_session=False)
mqttc.on_message = on_message
mqttc.connect("broker.hivemq.com", 1883, 60)
mqttc.subscribe("DataMgmt", qos=1)

mqttc.loop_forever()