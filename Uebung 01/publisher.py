import paho.mqtt.client as mqtt
import time
import random
import uuid
import json

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, uuid.uuid4().hex, clean_session=False)
mqttc.connect("broker.hivemq.com", 1883, 60)

while True:
    data = {
        "fin" : "SNTU411STM5032980",
        "zeit" : int (time.time()),
        "geschwindigkeit" : int (random.randint(0, 50))
    }

    data = json.dumps(data)
    print(data)
    mqttc.publish("DataMgmt", str(data), qos=1)
    
    time.sleep(5)
    