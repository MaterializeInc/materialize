Materialize uses [Kafka
transactions](https://www.confluent.io/blog/transactions-apache-kafka/). When
Kafka transactions are used, special control messages known as **transaction
markers** are published to the topic. Transaction markers inform both the broker
and clients about the status of a transaction. When a topic is read using a
standard Kafka consumer, these markers are not exposed to the application, which
can give the impression that some offsets are being skipped.
