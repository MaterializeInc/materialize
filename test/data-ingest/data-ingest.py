from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

def idfn(d, ctx):
    return d

def delivery_report(err, msg):
    assert(err is not None, f"Delivery failed for User record {msg.key()}: {err}")
    #print('User record {} successfully produced to {} [{}] at offset {}'.format(
    #    msg.key(), msg.topic(), msg.partition(), msg.offset()))

def main():
    with open(f"data-ingest/user.avsc") as f:
        schema_str = f.read()

    with open(f"data-ingest/key.avsc") as f:
        key_schema_str = f.read()

    # docker port data-ingest-schema-registry-1 8081
    schema_registry_conf = {'url': "http://schema-registry:8081/"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     idfn)

    key_avro_serializer = AvroSerializer(schema_registry_client,
                                         key_schema_str,
                                         idfn)

    # docker port data-ingest-kafka-1 9092
    producer_conf = {'bootstrap.servers': "kafka:9092"}
    producer = Producer(producer_conf)

    producer.poll(0.0)
    # Have to copy
    topic = "testdrive-upsert-insert-2074592892"
    # 6 seconds for 1 million productions, 18k/s, should be performant enough
    # 1 Python threads can load 15 clusterd threads at ~100% each
    for i in range(1000):
        producer.produce(topic=topic,
                         #partition=0,
                         key=key_avro_serializer({"key1": i"A{i}"}, SerializationContext(topic, MessageField.KEY)),
                        value=avro_serializer({"f1": i"A{i*2}"}, SerializationContext(topic, MessageField.VALUE)),
                        on_delivery=delivery_report)
        if i % 100_000 == 0:
            producer.flush()
    producer.flush()

main()
