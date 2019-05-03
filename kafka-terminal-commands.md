### Kafka terminal commands reference

Start server (and detach from terminal).
```
sudo /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties &
```

Stop kafka server.
```
sudo /opt/kafka/bin/kafka-server-stop.sh
```

Create a __topic__ ```test``` with 1 partition and replication factor of 3.
```
sudo /opt/kafka/bin/kafka-topics.sh --create --zookeeper 10.142.0.59:2181,10.142.0.60:2181,10.142.0.61:2181 --replication-factor 3 --partitions 1 --topic test
```

List all topics
```
sudo /opt/kafka/bin/kafka-topics.sh --list --zookeeper 10.142.0.59:2181,10.142.0.60:2181,10.142.0.61:2181
```

Describe the topic ```test```.
```
sudo /opt/kafka/bin/kafka-topics.sh --zookeeper 10.142.0.59:2181,10.142.0.60:2181,10.142.0.61:2181 --describe --topic test
```

Kafka console consumer for topic ```test```.
```
sudo /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server 10.142.0.59:9092,10.142.0.60:9092,10.142.0.61:9092 --topic test --from-beginning
```

Kafka console producer for topic ```test```.
```
sudo /opt/kafka/bin/kafka-console-producer.sh --broker-list 10.142.0.59:9092,10.142.0.60:9092,10.142.0.61:9092 --topic test
```

Delete the topic ```test```.
```
sudo /opt/kafka/bin/kafka-topics.sh --zookeeper 10.142.0.59:2181,10.142.0.60:2181,10.142.0.61:2181 --delete --topic test
```
