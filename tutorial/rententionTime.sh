# List all messages under one topic
kafka-console-consumer.sh --bootstrap-server `docker-machine ip bigdata`:9092 --topic stock-analyzer --from-beginning

# Update retention time
kafka-configs.sh --zookeeper `docker-machine ip bigdata`:2181 --entity-type topics --alter --entity-name stock-analyzer --add-config retention.ms=1000

# Restore retention time to default
kafka-topics.sh --zookeeper `docker-machine ip bigdata`:2181 --alter --topic stock-analyzer --delete-config retention.ms