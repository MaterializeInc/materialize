$ set keyschema-1-int-key={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "int"}
    ]
  }

$ set keyschema-1-long-key={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "long"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"nokey", "type":"string"}
        ]
    }

$ kafka-create-topic topic=int2long

$ kafka-ingest format=avro topic=int2long key-format=avro key-schema=${keyschema-1-int-key} schema=${schema} publish=true
{"key": 1234} {"nokey": "nokey1"}

> CREATE MATERIALIZED SOURCE int2long
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-int2long-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT

$ kafka-ingest format=avro topic=int2long key-format=avro key-schema=${keyschema-1-long-key} schema=${schema} publish=true
{"key": 999999999999} {"nokey": "nokey1"}

! SELECT * FROM int2long
Schemas don't match: Long, Int
