FROM apache/kafka:latest

USER root

COPY src/kafka_init/kafka_init.sh /kafka_init.sh
RUN chmod +x /kafka_init.sh

ENTRYPOINT ["/kafka_init.sh"]