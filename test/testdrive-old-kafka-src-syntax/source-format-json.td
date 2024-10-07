# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Verify behavior of FORMAT JSON

$ kafka-create-topic topic=data partitions=1
$ kafka-ingest format=bytes topic=data
{"a":"b","c":"d"}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

! CREATE SOURCE data
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  FORMAT JSON ARRAY;
contains:JSON ARRAY format in sources not yet supported

> CREATE SOURCE data
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  FORMAT JSON;

> SELECT DISTINCT pg_typeof(data) FROM data;
jsonb

> SELECT * FROM data
"{\"a\":\"b\",\"c\":\"d\"}"

# It's a dict so this is not just a string masquerading as JSON
> SELECT data -> 'a' FROM data;
"\"b\""

$ kafka-ingest format=bytes topic=data
[1,2,3]
1
1.23
"hello"
""

> SELECT * FROM data
[1,2,3]
1
1.23
"\"hello\""
"\"\""
"{\"a\":\"b\",\"c\":\"d\"}"

$ kafka-ingest format=bytes topic=data
{ "@timestamp":"2015-06-03T22:20:44.000Z", "latitude":39.613658, "longitude":4.9E-324, "location":[-86.106653,39.613658] }

!SELECT * FROM data
exact:Decode error: Failed to decode JSON: "4.9E-324" is out of range for type numeric: exceeds maximum precision 39 at line 1 column 85 (original text: { "@timestamp":"2015-06-03T22:20:44.000Z", "latitude":39.613658, "longitude":4.9E-324, "location":[-86.106653,39.613658] }, original bytes: "7b20224074696d657374616d70223a22323031352d30362d30335432323a32303a34342e3030305a222c20226c61746974756465223a33392e3631333635382c20226c6f6e676974756465223a342e39452d3332342c20226c6f636174696f6e223a5b2d38362e3130363635332c33392e3631333635385d207d")
