# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=static
$ kafka-ingest topic=static format=bytes
city,state,zip
Rochester,NY,14618
New York,NY,10004
"bad,
place""",CA,92679

# We should refuse to create a source with invalid WITH options
! CREATE SOURCE invalid_with_option
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-static-${testdrive.seed}'
  WITH (badoption=true)
  FORMAT CSV WITH 3 COLUMNS
contains:unexpected parameters for CREATE SOURCE: badoption

> CREATE MATERIALIZED SOURCE matching_column_names
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-static-${testdrive.seed}'
  FORMAT CSV WITH HEADER (city, state, zip)

> SELECT * FROM matching_column_names where zip = '14618'
city state zip mz_line_no
-------------------------
Rochester NY 14618 1

> CREATE MATERIALIZED SOURCE matching_column_names_alias (a, b, c)
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-static-${testdrive.seed}'
  FORMAT CSV WITH HEADER (city, state, zip)

> SELECT * FROM matching_column_names_alias where c = '14618'
a b c mz_line_no
----------------
Rochester NY 14618 1

# Static CSV without headers.
> CREATE MATERIALIZED SOURCE static_csv
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-static-${testdrive.seed}'
  FORMAT CSV WITH 3 COLUMNS

> SELECT * FROM static_csv
column1        column2  column3  mz_line_no
--------------------------------------------
city           state     zip     1
Rochester      NY        14618   2
"New York"     NY        10004   3
"bad,\nplace\""  CA        92679   4

> CREATE SOURCE static_csv_nothing_demanded_src
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-static-${testdrive.seed}'
  FORMAT CSV WITH 3 COLUMNS

> CREATE MATERIALIZED VIEW static_csv_nothing_demanded AS
  SELECT true FROM static_csv_nothing_demanded_src

> SELECT * FROM static_csv_nothing_demanded
true
true
true
true

# The timestamp chosen when reading from a static CSV should be the end of time,
# since the definition of "static" means "will never change again".
> SELECT count(*), mz_logical_timestamp() FROM static_csv
4  18446744073709551615

# Static malformed CSV
$ kafka-create-topic topic=malformed
$ kafka-ingest topic=malformed format=bytes
Dollars,Category
5161669,Clothing&Shoes
1000000000
,badrow
badint,

> CREATE MATERIALIZED SOURCE malformed_csv
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-malformed-${testdrive.seed}'
  FORMAT CSV WITH 2 COLUMNS

! SELECT * FROM malformed_csv
contains:Decode error: Text: CSV error at record number 3: expected 2 columns, got 1.

# Static non-utf-8 CSV
$ kafka-create-topic topic=bad-text
$ kafka-ingest topic=bad-text format=bytes
Dollars,Category
5161669,\x80

> CREATE MATERIALIZED SOURCE bad_text_csv
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-bad-text-${testdrive.seed}'
  FORMAT CSV WITH 2 COLUMNS

! SELECT * FROM bad_text_csv
contains:Decode error: Text: CSV error at record number 2: invalid UTF-8

# CSV file with comma in header

$ kafka-create-topic topic=header-delim
$ kafka-ingest topic=header-delim format=bytes
1,blat

> CREATE MATERIALIZED SOURCE header_delimited
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-header-delim-${testdrive.seed}'
  FORMAT CSV WITH HEADERS ("interesting,id", name)

> SELECT * FROM header_delimited
interesting,id name mz_line_no
------------------------------
1 blat 1

# Declare a key constraint (PRIMARY KEY NOT ENFORCED)

$ kafka-create-topic topic=static-csv-pkne-sink

> CREATE MATERIALIZED SOURCE static_csv_pkne (PRIMARY KEY (zip) NOT ENFORCED)
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-static-${testdrive.seed}'
  FORMAT CSV WITH HEADER (city, state, zip)

> CREATE SINK static_csv_pkne_sink FROM static_csv_pkne
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'static-csv-pkne-sink'
  KEY (zip)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}' ENVELOPE UPSERT

$ kafka-verify format=avro sink=materialize.public.static_csv_pkne_sink sort-messages=true
{"zip": "10004"} {"city": "New York", "state": "NY", "zip": "10004", "mz_line_no": 2}
{"zip": "14618"} {"city": "Rochester", "state": "NY", "zip": "14618", "mz_line_no": 1}
{"zip": "92679"} {"city": "bad,\nplace\"", "state": "CA", "zip": "92679", "mz_line_no": 3}
