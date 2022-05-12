# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ s3-create-bucket bucket=test

$ s3-put-object bucket=test key=static.csv
city,state,zip
Rochester,NY,14618
New York,NY,10004
"bad,
place""",CA,92679

# We should refuse to create a source with invalid WITH options
! CREATE SOURCE invalid_with_option
  FROM S3 DISCOVER OBJECTS MATCHING 'static.csv' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}',
    badoption = true
  )
  FORMAT CSV WITH 3 COLUMNS
contains:unexpected parameters for CREATE SOURCE: badoption

> CREATE MATERIALIZED SOURCE mismatched_column_count
  FROM S3 DISCOVER OBJECTS MATCHING 'static.csv' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT CSV WITH 2 COLUMNS
! SELECT * FROM mismatched_column_count
contains:CSV error at record number 1: expected 2 columns, got 3.

> CREATE MATERIALIZED SOURCE matching_column_names
  FROM S3 DISCOVER OBJECTS MATCHING 'static.csv' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT CSV WITH HEADER (city, state, zip)

> SELECT * FROM matching_column_names where zip = '14618'
city state zip mz_record
-------------------------
Rochester NY 14618 1

> CREATE MATERIALIZED SOURCE matching_column_names_alias (a, b, c)
  FROM S3 DISCOVER OBJECTS MATCHING 'static.csv' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT CSV WITH HEADER (city, state, zip)

> SELECT * FROM matching_column_names_alias where c = '14618'
a b c mz_record
----------------
Rochester NY 14618 1

> CREATE MATERIALIZED SOURCE mismatched_column_names
  FROM S3 DISCOVER OBJECTS MATCHING 'static.csv' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT CSV WITH HEADER (cities, country, zip)
! SELECT * FROM mismatched_column_names
contains:first mismatched column at index 1 expected=cities actual="city"

> CREATE MATERIALIZED SOURCE mismatched_column_names_count
  FROM S3 DISCOVER OBJECTS MATCHING 'static.csv' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT CSV WITH HEADER (cities, state)
! SELECT * FROM mismatched_column_names_count
contains:CSV error at record number 1: expected 2 columns, got 3

# Static CSV without headers.
> CREATE MATERIALIZED SOURCE static_csv
  FROM S3 DISCOVER OBJECTS MATCHING 'static.csv' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT CSV WITH 3 COLUMNS

> SELECT * FROM static_csv
column1        column2  column3  mz_record
--------------------------------------------
city           state     zip     1
Rochester      NY        14618   2
"New York"     NY        10004   3
"bad,\nplace\""  CA        92679   4

! CREATE SOURCE static_csv_nothing_demanded_src
  FROM S3 DISCOVER OBJECTS MATCHING 'static.csv' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT CSV WITH HEADER
exact:Expected a list of columns in parentheses, found EOF

# The timestamp chosen when reading from a static CSV should be the end of time,
# since the definition of "static" means "will never change again".
> SELECT count(*), mz_logical_timestamp() FROM static_csv
4  18446744073709551615

# Static CSV with manual headers.
> CREATE MATERIALIZED SOURCE static_csv_manual_header (city_man, state_man, zip_man)
  FROM S3 DISCOVER OBJECTS MATCHING 'static.csv' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT CSV WITH HEADER (city, state, zip)

> SELECT * FROM static_csv_manual_header
city_man       state_man  zip_man  mz_record
---------------------------------------------
Rochester      NY         14618    1
"New York"     NY         10004    2
"bad,\nplace\""  CA         92679    3

# Dynamic CSV with automatic headers.

$ kafka-create-topic topic=dynamic

> CREATE MATERIALIZED SOURCE dynamic_csv (city, state, zip)
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-dynamic-${testdrive.seed}'
  FORMAT CSV WITH 3 COLUMNS

> SELECT * FROM dynamic_csv

$ kafka-ingest topic=dynamic format=bytes
Rochester,NY,14618

> SELECT * FROM dynamic_csv
city           state     zip     mz_offset
------------------------------------------
Rochester      NY        14618   1

$ kafka-ingest topic=dynamic format=bytes
New York,NY,10004

> SELECT * FROM dynamic_csv
city           state     zip     mz_offset
------------------------------------------
Rochester      NY        14618   1
"New York"     NY        10004   2

# Static malformed CSV
$ s3-put-object bucket=test key=malformed.csv
dollars,category
5161669,Clothing&Shoes
1000000000
,badrow
badint,

> CREATE MATERIALIZED SOURCE malformed_csv
  FROM S3 DISCOVER OBJECTS MATCHING 'malformed.csv' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT CSV WITH HEADER (dollars, category)

! SELECT * FROM malformed_csv
contains:Decode error: Text: CSV error at record number 3: expected 2 columns, got 1.

# Static non-utf-8 CSV
$ s3-put-object bucket=test key=bad-text.csv
dollars,category
5161669,\x80

> CREATE MATERIALIZED SOURCE bad_text_csv
  FROM S3 DISCOVER OBJECTS MATCHING 'bad-text.csv' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT CSV WITH HEADER (dollars, category)

! SELECT * FROM bad_text_csv
contains:Decode error: Text: CSV error at record number 2: invalid UTF-8

# Declare a key constraint (PRIMARY KEY NOT ENFORCED)

$ kafka-create-topic topic=static-csv-pkne-sink

> CREATE MATERIALIZED SOURCE static_csv_pkne (PRIMARY KEY (zip) NOT ENFORCED)
  FROM S3 DISCOVER OBJECTS MATCHING 'static.csv' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT CSV WITH HEADER (city, state, zip)

> CREATE SINK static_csv_pkne_sink FROM static_csv_pkne
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'static-csv-pkne-sink'
  KEY (zip)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}' ENVELOPE UPSERT

$ kafka-verify format=avro sink=materialize.public.static_csv_pkne_sink sort-messages=true
{"zip": "10004"} {"city": "New York", "state": "NY", "zip": "10004", "mz_record": 2}
{"zip": "14618"} {"city": "Rochester", "state": "NY", "zip": "14618", "mz_record": 1}
{"zip": "92679"} {"city": "bad,\nplace\"", "state": "CA", "zip": "92679", "mz_record": 3}
