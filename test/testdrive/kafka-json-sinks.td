# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE MATERIALIZED VIEW simple_view AS SELECT 1 AS a, 2 AS b, 3 AS c;

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE SINK simple_view_sink FROM simple_view
  INTO KAFKA CONNECTION kafka_conn TOPIC 'unnamed-cols-sink'
  FORMAT JSON

$ kafka-verify format=json sink=materialize.public.simple_view_sink key=false
{"before": null, "after": {"a": 1, "b": 2, "c": 3}}

> CREATE SINK simple_view_upsert FROM simple_view
  INTO KAFKA CONNECTION kafka_conn TOPIC 'unnamed-upsert'
  KEY (b)
  FORMAT JSON
  ENVELOPE UPSERT

$ kafka-verify format=json sink=materialize.public.simple_view_upsert key=true
{"b": 2} {"a": 1, "b": 2, "c": 3}

# Standard types

> CREATE MATERIALIZED VIEW types_view AS
  SELECT TRUE::boolean c1,
  FALSE::boolean c2,
  NULL c3,
  123456789::bigint c4,
  1234.5678::double c5,
  1234.5678::decimal c6,
  '2011-11-11 11:11:11.12345'::timestamp c7,
  '2011-11-11 11:11:11.12345+12'::timestamptz c8,
  '2011-11-11'::date c9,
  '11:11:11.123456'::time c10,
  INTERVAL '1 year' c11,
  '324373a5-7718-46b1-a7ea-4a7c9981fc4e'::uuid c12,
  'текст'::bytea c13,
  '{"a": 2}'::jsonb c14

> CREATE SINK types_sink FROM types_view
  INTO KAFKA CONNECTION kafka_conn TOPIC 'types-sink'
  FORMAT JSON

# Due to limitations in $ kafka-verify, the entire expected JSON output needs to be provided on a single line
$ kafka-verify format=json sink=materialize.public.types_sink key=false
{"before":null,"after":{"c1":true,"c2":false,"c3":null,"c4":123456789,"c5":1234.5678,"c6":"1234.5678","c7":"1321009871123","c8":"1320966671123","c9":"2011-11-11","c10":"11:11:11.123456","c11":"1 year","c12":"324373a5-7718-46b1-a7ea-4a7c9981fc4e","c13":[209,130,208,181,208,186,209,129,209,130],"c14":{"a":2}}}

# Special characters

> CREATE MATERIALIZED VIEW special_characters_view AS
  SELECT 'текст' c1, '"' c2, '''' c3, '\' c4, E'a\n\tb' c5

> CREATE SINK special_characters_sink FROM special_characters_view
  INTO KAFKA CONNECTION kafka_conn TOPIC 'special-characters-sink'
  FORMAT JSON

$ kafka-verify format=json sink=materialize.public.special_characters_sink key=false
{"before":null,"after":{"c1":"текст","c2":"\"","c3":"'","c4":"\\","c5":"a\n\tb"}}

# Record

> CREATE MATERIALIZED VIEW record_view AS SELECT simple_view FROM simple_view;

> CREATE SINK record_sink FROM record_view
  INTO KAFKA CONNECTION kafka_conn TOPIC 'record-sink'
  FORMAT JSON

$ kafka-verify format=json sink=materialize.public.record_sink key=false
{"before":null,"after":{"simple_view":{"a":1,"b":2,"c":3}}}

# Duplicate column names
! CREATE VIEW duplicate_cols AS SELECT 'a1' AS a, 'a1' AS a;
contains:column "a" specified more than once

! CREATE MATERIALIZED VIEW duplicate_cols AS SELECT 'a1' AS a, 'a1' AS a;
contains:column "a" specified more than once

# Complex types

> CREATE TYPE int4_list AS LIST (element_type = int4);

> CREATE TYPE int4_list_list AS LIST (element_type = int4_list);

> CREATE TYPE int4_map AS MAP (key_type=text, value_type=int4);

> CREATE TYPE int4_map_map AS MAP (key_type=text, value_type=int4_map);

> CREATE MATERIALIZED VIEW complex_type_view AS SELECT '{{1,2},{3,4}}'::int4_list_list c1, '{a=>{b=>1, c=>2}, d=> {e=>3, f=>4}}'::int4_map_map c2;

> CREATE SINK complex_type_sink FROM complex_type_view
  INTO KAFKA CONNECTION kafka_conn TOPIC 'complex-type-sink'
  FORMAT JSON

$ kafka-verify format=json sink=materialize.public.complex_type_sink key=false
{"before": null, "after": {"c1": [[1,2],[3,4]], "c2": {"a":{"b":1, "c":2}, "d": {"e":3, "f":4}}}}

# testdrive will not automatically clean up types, so we need to do that ourselves

> DROP MATERIALIZED VIEW complex_type_view CASCADE;

> DROP TYPE int4_list_list;

> DROP TYPE int4_list;

> DROP TYPE int4_map_map;

> DROP TYPE int4_map;
