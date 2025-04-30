import paho.mqtt.client as mqtt

# Create a client instance
mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)  # Inizzializzazione del client

# Connect to a broker using one of the connect*() functions
mqttc.connect("localhost", 1883, 60)

# Subscribe to a topic
mqttc.subscribe("sensors/heart")

# Use publish() to publish messages to the broker
mqttc.publish("sensors/heart", "Hello World")

# disconnect
mqttc.disconnect()

