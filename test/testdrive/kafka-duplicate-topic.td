$ set schema={"type" : "record", "name" : "test", "fields": [ { "name": "f2", "type": "long" } ] }

$ kafka-create-topic topic=topic0

$ kafka-create-topic topic=topic1

$ kafka-ingest format=avro topic=topic0 schema=${schema} publish=true repeat=1
{"f2": 1}

$ kafka-ingest format=avro topic=topic1 schema=${schema} publish=true repeat=1
{"f2": 1}

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE SOURCE source0
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-topic0-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE NONE

> CREATE SOURCE source1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-topic1-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE NONE

> CREATE SINK sink0 FROM source0
  INTO KAFKA CONNECTION kafka_conn TOPIC 'testdrive-sink-output-${testdrive.seed}'
  KEY (f2)
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK sink1 FROM source1
  INTO KAFKA CONNECTION kafka_conn TOPIC 'testdrive-sink-output-${testdrive.seed}'
  KEY (f2)
  WITH (reuse_topic=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
