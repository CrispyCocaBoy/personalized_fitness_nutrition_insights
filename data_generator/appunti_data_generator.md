## To-do List

### Creazione publisher con mosquitto
Per utilizzare mosquitto:
- Broker (es. Mosquitto): is the central server that send the message to the other client. 
	`mosquitto -v`
- Publisher: a client that send the message in a topic
	`mosquitto_sub -h localhost -t sensors/hearts`
- Subscriber: is another client that subscribe to that topic and that receive the message
	`mosquitto_pub -h localhost -t sensors/hearts -m "<message>"`
