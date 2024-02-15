import paho.mqtt.client as mqtt
import psycopg2
from dotenv import load_dotenv
import os
import json

# Lade die Umgebungsvariablen aus der .env-Datei
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
    try:
        # Konvertiere Byte-Nachricht zu einem String und dann zu einem JSON-String
        # Dies ist notwendig, da die Payload als Bytes ankommt
        

        # Ersetze 'deine_tabelle' mit dem Namen deiner Zieltabelle
        # und passe die Spaltennamen entsprechend an
        cur.execute("INSERT INTO staging.messung (payload) VALUES (%s)", (message,))
        conn.commit()
    except Exception as e:
        print(f"Fehler beim Einfügen in die Datenbank: {e}")
    finally:
        cur.close()
        conn.close()


def on_message(client, userdata, message):
    print(message.payload)
    message = message.payload.decode()
    #message = json.loads(message)
    #print(message)
    #print(type (message))
    insert_into_database(message)
    #print( type (str (message.payload)))

#<JSON Message in DB-Tabelle staging.messung schreiben>
mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "23875439", clean_session=False)
mqttc.on_message = on_message
mqttc.connect("broker.hivemq.com", 1883, 60)
mqttc.subscribe("DataMgmt", qos=1)

#… (unvollständig, z.B. Schleife ergänzen)



mqttc.loop_start()

# Damit das Skript weiterläuft und Nachrichten empfangen kann, benötigen wir eine Art Endlosschleife
try:
    while True:
        # Hier könnte Logik eingefügt werden, die während des Wartens auf Nachrichten ausgeführt werden soll
        pass
except KeyboardInterrupt:
    print("Programm durch Benutzerunterbrechung beendet.")

# Stoppe die Loop, wenn das Skript beendet wird
mqttc.loop_stop()