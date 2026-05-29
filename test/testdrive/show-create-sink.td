$ set-arg-default default-replica-size=scale=1,workers=1
$ set-arg-default single-replica-cluster=quickstart

$ kafka-create-topic topic=data

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE TABLE table (a int);

> CREATE SINK sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM table
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-show-create-sink-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM;

> ALTER SINK sink SET FROM table; --bumps version

> SELECT create_sql FROM (SHOW CREATE SINK sink);
"CREATE SINK materialize.public.sink\nIN CLUSTER quickstart\nFROM materialize.public.table\nINTO\n    KAFKA CONNECTION materialize.public.kafka_conn (TOPIC = 'testdrive-show-create-sink-${testdrive.seed}')\nFORMAT JSON\nENVELOPE DEBEZIUM;"