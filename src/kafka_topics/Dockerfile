FROM apache/kafka:latest

USER root

COPY src/kafka_topics/init_kafka.sh /init_kafka.sh
RUN chmod +x /init_kafka.sh

ENTRYPOINT ["/init_kafka.sh"]


