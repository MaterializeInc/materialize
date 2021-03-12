$ set consistency={
     "name": "materialize.byo.consistency",
     "type": "record",
     "fields": [
         {
           "name": "source",
           "type": "string"
         },
         {
           "name": "partition_count",
           "type": "int"
         },
         {
           "name": "partition_id",
           "type": ["int","string"]
         },
         {
            "name": "timestamp",
            "type": "long"
         },
         {
           "name": "offset",
           "type": "long"
         }
     ]
  }

$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "long"}
    ]
  }

$ kafka-create-topic topic=input-consistency

$ kafka-create-topic topic=input

$ kafka-ingest format=avro topic=input-consistency schema=${consistency}
{"source": "testdrive-input-${testdrive.seed}", "partition_count": 1, "partition_id": {"int": 0}, "timestamp": 1, "offset": 4}

$ kafka-ingest format=avro topic=input schema=${schema} timestamp=1
{"a": 1, "b": 1}
{"a": 2, "b": 1}
{"a": 3, "b": 1}
{"a": 1, "b": 2}


> CREATE MATERIALIZED SOURCE input_byo
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input-${testdrive.seed}'
    WITH (consistency = 'testdrive-input-consistency-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}'

> SELECT * FROM input_byo
a  b
------
1   1
2   1
3   1
1   2

> CREATE SINK output FROM input_byo
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'output-sink-${testdrive.seed}'
  WITH (exactly_once=true, consistency=true)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.output
{"before": null, "after": {"row": {"a": 1, "b": 1}}, "transaction": {"id": "1"}}
{"before": null, "after": {"row": {"a": 1, "b": 2}}, "transaction": {"id": "1"}}
{"before": null, "after": {"row": {"a": 2, "b": 1}}, "transaction": {"id": "1"}}
{"before": null, "after": {"row": {"a": 3, "b": 1}}, "transaction": {"id": "1"}}
