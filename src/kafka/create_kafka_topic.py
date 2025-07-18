from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Configurazione del broker Kafka
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"  # hostname del broker Kafka nel docker network

# Parametri del topic
TOPIC_NAME = "wearables.bpm"
NUM_PARTITIONS = 1
REPLICATION_FACTOR = 1  # perché stai usando un broker singolo

def create_kafka_topic():
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="topic_initializer"
    )

    topic = NewTopic(
        name=TOPIC_NAME,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR
    )

    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"✅ Topic '{TOPIC_NAME}' creato con successo.")
    except TopicAlreadyExistsError:
        print(f"ℹ️ Topic '{TOPIC_NAME}' esiste già.")
    finally:
        admin_client.close()

if __name__ == "__main__":
    create_kafka_topic()
