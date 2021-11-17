# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE VIEW unnamed_cols AS SELECT 1, 2 AS b, 3;

> CREATE SINK unnamed_cols_sink FROM unnamed_cols
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'unnamed-cols-sink'
  FORMAT JSON

$ kafka-verify format=json sink=materialize.public.unnamed_cols_sink key=false
{"before": null, "after": {"row": {"column1": 1, "b": 2, "column3": 3}}}

> CREATE SINK unnamed_cols_upsert FROM unnamed_cols
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'unnamed-upsert'
  KEY (b)
  FORMAT JSON
  ENVELOPE UPSERT

$ kafka-verify format=json sink=materialize.public.unnamed_cols_upsert key=true
{"b": 2} {"column1": 1, "b": 2, "column3": 3}

# Standard types

> CREATE VIEW types_view AS
  SELECT TRUE::boolean, FALSE::boolean, NULL,
  123456789::bigint, 1234.5678::double, 1234.5678::decimal,
  '2011-11-11 11:11:11.12345'::timestamp, '2011-11-11 11:11:11.12345+12'::timestamptz,
  '2011-11-11'::date, '11:11:11.123456'::time,
  INTERVAL '1 year',
  '324373a5-7718-46b1-a7ea-4a7c9981fc4e'::uuid,
  'текст'::bytea,
  '{"a": 2}'::jsonb

> CREATE SINK types_sink FROM types_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'types-sink'
  FORMAT JSON

# Due to limitations in $ kafka-verify, the entire expected JSON output needs to be provided on a single line
$ kafka-verify format=json sink=materialize.public.types_sink key=false
{"after":{"row":{"column1":true,"column10":"11:11:11.123456","column11":"1 year","column12":"324373a5-7718-46b1-a7ea-4a7c9981fc4e","column13":[209,130,208,181,208,186,209,129,209,130],"column14":{"a":2},"column2":false,"column3":null,"column4":123456789,"column5":1234.5678,"column6":"1234.5678","column7":"1321009871123","column8":"1320966671123","column9":"2011-11-11"}},"before":null}

# Special characters

> CREATE VIEW special_characters_view AS
  SELECT 'текст', '"', '''', '\', E'a\n\tb'

> CREATE SINK special_characters_sink FROM special_characters_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'special-characters-sink'
  FORMAT JSON

$ kafka-verify format=json sink=materialize.public.special_characters_sink key=false
{"after":{"row":{"column1":"текст","column2":"\"","column3":"'","column4":"\\","column5":"a\n\tb"}},"before":null}

# Record

> CREATE VIEW record_view AS SELECT unnamed_cols FROM unnamed_cols;

> CREATE SINK record_sink FROM record_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'record-sink'
  FORMAT JSON

$ kafka-verify format=json sink=materialize.public.record_sink key=false
{"after":{"row":{"unnamed_cols":{"record0":{"b":2,"f1":1,"f3":3}}}},"before":null}

# Duplicate column names
! CREATE VIEW duplicate_cols AS SELECT 'a1' AS a, 'a1' AS a;
column "a" specified more than once

# Complex types

> CREATE TYPE int4_list AS LIST (element_type = int4);

> CREATE TYPE int4_list_list AS LIST (element_type = int4_list);

> CREATE TYPE int4_map AS MAP (key_type=text, value_type=int4);

> CREATE TYPE int4_map_map AS MAP (key_type=text, value_type=int4_map);

> CREATE VIEW complex_type_view AS SELECT '{{1,2},{3,4}}'::int4_list_list, '{a=>{b=>1, c=>2}, d=> {e=>3, f=>4}}'::int4_map_map;

> CREATE SINK complex_type_sink FROM complex_type_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'complex-type-sink'
  FORMAT JSON

$ kafka-verify format=json sink=materialize.public.complex_type_sink key=false
{"before": null, "after": {"row": {"column1": [[1,2],[3,4]], "column2": {"a":{"b":1, "c":2}, "d": {"e":3, "f":4}}}}}

# testdrive will not automatically clean up types, so we need to do that ourselves

> DROP VIEW complex_type_view CASCADE;

> DROP TYPE int4_list_list;

> DROP TYPE int4_list;

> DROP TYPE int4_map_map;

> DROP TYPE int4_map;
