import paho.mqtt.client as mqtt
import time
import random

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "23875438", clean_session=False)
mqttc.connect("broker.hivemq.com", 1883, 60)





while True:
    data = {
    
    "fin" : "SNTU411STM5032980",
    "zeit" : int (time.time()),
    "geschwindigkeit" : int (random.randrange(0, 50, 1))
    
    }
    print(data)
    print(str (data))
    
    mqttc.publish("DataMgmt", str (data), qos=1)
    
    time.sleep(5)
    



