# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This testdrive file is testing the order by clause with envelope upsert

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

$ kafka-create-topic topic=textbytes

> CREATE SOURCE texttext
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-textbytes-${testdrive.seed}')
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  INCLUDE OFFSET as o, TIMESTAMP as ts
  ENVELOPE UPSERT ( ORDER BY ( ts ASC, o ))
  WITH (SIZE = '1')

> SHOW CREATE SOURCE texttext
materialize.public.texttext "CREATE SOURCE \"materialize\".\"public\".\"texttext\" FROM KAFKA CONNECTION \"materialize\".\"public\".\"kafka_conn\" (TOPIC = 'testdrive-textbytes-${testdrive.seed}') KEY FORMAT TEXT VALUE FORMAT TEXT INCLUDE OFFSET AS \"o\", TIMESTAMP AS \"ts\" ENVELOPE UPSERT ( ORDER BY (\"ts\", \"o\")) EXPOSE PROGRESS AS \"materialize\".\"public\".\"texttext_progress\" WITH (SIZE = '1')"

# Ingesting values, later offset will override earlier values for the same timestamp
$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=: timestamp=185
fish:fish_185
bird:bird_185
fruit:fruit_185
fruit:new_fruit_185

> select key, text, ts from texttext
key           text         ts
------------------------------------------------
fish         fish_185      "1970-01-01 00:00:00.185"
bird         bird_185      "1970-01-01 00:00:00.185"
fruit        new_fruit_185 "1970-01-01 00:00:00.185"

# Values will higher timestamp, should remove bird and update the rest
$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=: timestamp=278
fish:fish_278
mammal:mammal_278
fruit:fruit_278
bird:

> select key, text, ts from texttext
key           text            ts
------------------------------------------------
fruit         fruit_278       "1970-01-01 00:00:00.278"
fish          fish_278        "1970-01-01 00:00:00.278"
mammal        mammal_278      "1970-01-01 00:00:00.278"

# ingesting again with same timestamp, will remove mammal and update fish
$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=: timestamp=278
mammal:
fish:new_fish_278

> select key, text, ts from texttext
key           text            ts
------------------------------------------------
fruit         fruit_278       "1970-01-01 00:00:00.278"
fish          new_fish_278    "1970-01-01 00:00:00.278"

# ingesting with lower timestamp, only bird will be added, fish and fruit will remain unchanged
$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=: timestamp=191
fish:fish_191
bird:bird_191
fruit:

> select key, text, ts from texttext
key           text      ts
------------------------------------------------
fruit         fruit_278      "1970-01-01 00:00:00.278"
fish          new_fish_278   "1970-01-01 00:00:00.278"
bird          bird_191       "1970-01-01 00:00:00.191"

# Invalid source definitions
! CREATE SOURCE bad_def FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-textbytes-${testdrive.seed}')
  KEY FORMAT TEXT VALUE FORMAT TEXT INCLUDE TIMESTAMP
  ENVELOPE UPSERT (ORDER BY ( KEY )) WITH (SIZE = '1')
contains:Could not find key in included metadata

! CREATE SOURCE bad_def FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-textbytes-${testdrive.seed}')
  KEY FORMAT TEXT VALUE FORMAT TEXT INCLUDE TIMESTAMP, PARTITION as p
  ENVELOPE UPSERT (ORDER BY (p)) WITH (SIZE = '1')
contains:Only TIMESTAMP and OFFSET are supported as order by columns

! CREATE SOURCE bad_def FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-textbytes-${testdrive.seed}')
  KEY FORMAT TEXT VALUE FORMAT TEXT INCLUDE TIMESTAMP as ts, OFFSET
  ENVELOPE UPSERT (ORDER BY (ts)) WITH (SIZE = '1')
contains:OFFSET must be explicitly specified as an order by column

# no parentheses around ORDER BY
! CREATE SOURCE bad_def FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-textbytes-${testdrive.seed}')
  KEY FORMAT TEXT VALUE FORMAT TEXT INCLUDE TIMESTAMP, PARTITION as p
  ENVELOPE UPSERT ORDER BY p WITH (SIZE = '1')
contains:Expected end of statement

# no parentheses around ts in ORDER BY
! CREATE SOURCE bad_def FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-textbytes-${testdrive.seed}')
  KEY FORMAT TEXT VALUE FORMAT TEXT INCLUDE TIMESTAMP as ts
  ENVELOPE UPSERT ( ORDER BY ts ) WITH (SIZE = '1')
contains:Expected left parenthesis

# DESC ordering not allowed
! CREATE SOURCE bad_def FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-textbytes-${testdrive.seed}')
  KEY FORMAT TEXT VALUE FORMAT TEXT INCLUDE TIMESTAMP as ts
  ENVELOPE UPSERT ( ORDER BY (ts DESC)) WITH (SIZE = '1')
contains:Expected right parenthesis

# ENVELOPE NONE does not support ORDER BY
! CREATE SOURCE bad_def FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-textbytes-${testdrive.seed}')
  KEY FORMAT TEXT VALUE FORMAT TEXT INCLUDE TIMESTAMP, PARTITION as p
  ENVELOPE NONE (ORDER BY p) WITH (SIZE = '1')
contains:Expected end of statement
