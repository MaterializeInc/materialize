# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the
# root of this repository, or online at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dbt.tests.adapter.hooks import test_model_hooks as core_base

test_materialized_view = """
{{ config(materialized='materialized_view') }}

    SELECT * FROM (VALUES ('chicken', 'pig', 'bird'), ('cow', 'horse', 'bird'), (NULL, NULL, NULL)) _ (a, b, c)
"""

test_materialized_view_index = """
{{ config(
    materialized='materialized_view',
    indexes=[{'columns': ['a', 'length(a)'], 'name': 'a_idx'}]
) }}

    SELECT * FROM (VALUES ('chicken', 'pig'), ('cow', 'horse')) _ (a, b)
"""

test_materialized_view_deploy = """
{{ config(materialized='materialized_view') }}

    SELECT val + 1 AS val FROM source_table
"""

test_view = """
{{ config(materialized='view') }}

    SELECT * FROM (VALUES ('chicken', 'pig', 'bird'), ('cow', 'horse', 'bird'), (NULL, NULL, NULL)) _ (a, b, c)
"""

test_view_index = """
{{ config(
    materialized='view',
    indexes=[{'default': True}]
) }}

    SELECT * FROM (VALUES ('chicken', 'pig'), ('cow', 'horse'), (NULL, NULL)) _ (a, b)
"""

test_table_index = """
{{ config(
    materialized='table',
    indexes=[{'columns': ['a', 'length(a)'], 'name': 'test_table_index_a_idx'}]
) }}

    SELECT * FROM (VALUES ('chicken', 'pig'), ('cow', 'horse')) _ (a, b)
"""

test_seed = """
id,value
1,100
2,200
3,300
""".strip()

test_source = """
{{ config(
    materialized='source',
    database='materialize',
    pre_hook="CREATE CONNECTION IF NOT EXISTS kafka_connection TO KAFKA (BROKER '{{ env_var('KAFKA_ADDR', 'localhost:9092') }}', SECURITY PROTOCOL PLAINTEXT)"
    )
}}
FROM KAFKA CONNECTION kafka_connection (TOPIC 'testdrive-test-source-1')
FORMAT BYTES
"""

test_source_index = """
{{ config(
    materialized='source',
    indexes=[{'columns': ['data']}]
) }}
FROM KAFKA CONNECTION kafka_connection (TOPIC 'testdrive-test-source-1')
FORMAT BYTES
"""

test_source_cluster = """
{{ config(
    materialized='source',
    cluster='quickstart'
) }}
FROM KAFKA CONNECTION kafka_connection (TOPIC 'testdrive-test-source-1')
FORMAT BYTES
"""

test_source_table = """
{{ config(
    materialized='source_table',
    database='materialize'
) }}
FROM SOURCE {{ ref('test_subsources') }}
(REFERENCE "bids")
"""

test_source_table_index = """
{{ config(
    materialized='source_table',
    database='materialize',
    indexes=[{'columns': ['amount']}]
) }}
FROM SOURCE {{ ref('test_subsources') }}
(REFERENCE "bids")
"""

test_subsources = """
{{ config(
    materialized='source',
    database='materialize'
    )
}}
FROM LOAD GENERATOR AUCTION
FOR ALL TABLES;
"""

test_sink = """
{{ config(
    materialized='sink',
    pre_hook="CREATE CONNECTION IF NOT EXISTS kafka_connection TO KAFKA (BROKER '{{ env_var('KAFKA_ADDR', 'localhost:9092') }}', SECURITY PROTOCOL PLAINTEXT)"
    )
}}
 FROM {{ ref('test_materialized_view') }}
 INTO KAFKA CONNECTION kafka_connection (TOPIC 'test-sink')
 FORMAT JSON
 ENVELOPE DEBEZIUM
"""

test_sink_cluster = """
{{ config(
    materialized='sink',
    cluster='quickstart'
    )
}}
 FROM {{ ref('test_materialized_view') }}
 INTO KAFKA CONNECTION kafka_connection (TOPIC 'test-sink')
 FORMAT JSON
 ENVELOPE DEBEZIUM
"""

test_sink_deploy = """
{{ config(
    materialized='sink',
    schema='sinks_schema',
    cluster='sinks_cluster',
    pre_hook="CREATE CONNECTION IF NOT EXISTS kafka_connection TO KAFKA (BROKER '{{ env_var('KAFKA_ADDR', 'localhost:9092') }}', SECURITY PROTOCOL PLAINTEXT)"
    )
}}
 FROM {{ ref('test_materialized_view_deploy') }}
 INTO KAFKA CONNECTION kafka_connection (TOPIC 'testdrive-test-sink-1')
 FORMAT JSON
 ENVELOPE DEBEZIUM
"""

actual_indexes = """
SELECT
    o.name as object_name,
    ic.index_position::int8,
    ic.on_position::int8,
    ic.on_expression,
    i.name as index_name
FROM mz_indexes i
JOIN mz_index_columns ic ON i.id = ic.index_id
JOIN mz_objects o ON i.on_id = o.id
WHERE i.id LIKE 'u%'
"""

expected_indexes = """
object_name,index_position,on_position,on_expression,index_name
test_materialized_view_index,1,1,,a_idx
test_materialized_view_index,2,,pg_catalog.length(a),a_idx
test_source_index,1,1,,test_source_index_data_idx
test_view_index,1,1,,test_view_index_primary_idx
test_table_index,1,1,,test_table_index_a_idx
test_table_index,2,,pg_catalog.length(a),test_table_index_a_idx
test_source_table_index,1,4,,test_source_table_index_amount_idx
""".lstrip()

not_null = """
{{ config(store_failures=true, schema='test', alias='testnull') }}

    SELECT *
    FROM {{ ref('test_materialized_view') }}
    WHERE a IS NULL
"""

unique = """
{{ config(store_failures=true, schema='test', alias='testunique') }}

    SELECT
        a AS unique_field,
        count(*) AS num_records
    FROM {{ ref('test_materialized_view') }}
    WHERE a IS NOT NULL
    GROUP BY a
    HAVING count(*) > 1
"""

expected_base_relation_types = {
    "base": "materialized_view",
    "view_model": "view",
    "table_model": "materialized_view",
    "swappable": "materialized_view",
}

test_relation_name_length = """
{{ config(materialized='materialized_view') }}

    SELECT * FROM (VALUES ('chicken', 'pig'), ('cow', 'horse'), (NULL, NULL)) _ (a, b)
"""

test_hooks = {
    "models": {
        "test": {
            "pre-hook": [
                # inside transaction (runs second)
                core_base.MODEL_PRE_HOOK,
                # outside transaction (runs first)
                {
                    "sql": "select 1 from {{ this.schema }}.on_model_hook",
                    "transaction": False,
                },
            ],
            "post-hook": [
                # outside transaction (runs second)
                {
                    "sql": "select 1 from {{ this.schema }}.on_model_hook",
                    "transaction": False,
                },
                # inside transaction (runs first)
                core_base.MODEL_POST_HOOK,
            ],
        }
    }
}

test_run_operation = {
    # The create and drop table statements here validate that these hooks run
    # in the same order that they are defined. Drop before create is an error.
    # Also check that the table does not exist below.
    "on-run-start": [
        "{{ custom_run_hook('start', target, run_started_at, invocation_id) }}",
        "create table {{ target.schema }}.start_hook_order_test ( id int )",
        "drop table {{ target.schema }}.start_hook_order_test",
        "{{ log(env_var('TERM_TEST'), info=True) }}",
    ],
    "on-run-end": [
        "{{ custom_run_hook('end', target, run_started_at, invocation_id) }}",
        "create table {{ target.schema }}.end_hook_order_test ( id int )",
        "drop table {{ target.schema }}.end_hook_order_test",
        "create table {{ target.schema }}.schemas ( schema varchar )",
        "insert into {{ target.schema }}.schemas (schema) values {% for schema in schemas %}( '{{ schema }}' ){% if not loop.last %},{% endif %}{% endfor %}",
        "create table {{ target.schema }}.db_schemas ( db varchar, schema varchar )",
        "insert into {{ target.schema }}.db_schemas (db, schema) values {% for db, schema in database_schemas %}('{{ db }}', '{{ schema }}' ){% if not loop.last %},{% endif %}{% endfor %}",
    ],
    "seeds": {
        "quote_columns": False,
    },
}

model_hook = """
create table {schema}.on_model_hook (
    test_state       TEXT, -- start|end
    target_dbname    TEXT,
    target_host      TEXT,
    target_name      TEXT,
    target_schema    TEXT,
    target_type      TEXT,
    target_user      TEXT,
    target_pass      TEXT,
    target_threads   INTEGER,
    run_started_at   TEXT,
    invocation_id    TEXT,
    thread_id        TEXT
)
"""

run_hook = """
create table {schema}.on_run_hook (
    test_state       TEXT, -- start|end
    target_dbname    TEXT,
    target_host      TEXT,
    target_name      TEXT,
    target_schema    TEXT,
    target_type      TEXT,
    target_user      TEXT,
    target_pass      TEXT,
    target_threads   INTEGER,
    run_started_at   TEXT,
    invocation_id    TEXT,
    thread_id        TEXT
)
"""

nullability_assertions_schema_yml = """
version: 2
models:
  - name: test_nullability_assertions_ddl
    config:
      contract:
        enforced: true
    columns:
      - name: a
        data_type: string
        constraints:
          - type: not_null
      - name: b
        data_type: string
        constraints:
          - type: not_null
      - name: c
        data_type: string
"""

contract_invalid_cluster_schema_yml = """
version: 2
models:
  - name: test_view
    config:
      contract:
        enforced: true
    columns:
      - name: a
        data_type: string
        constraints:
          - type: not_null
      - name: b
        data_type: string
      - name: c
        data_type: string
"""

contract_pseudo_types_yml = """
version: 2
models:
  - name: test_pseudo_types
    config:
      contract:
        enforced: true
    columns:
      - name: a
        data_type: map
      - name: b
        data_type: record
      - name: c
        data_type: list
"""

test_pseudo_types = """
{{ config(materialized='view') }}

    SELECT '{a=>1, b=>2}'::map[text=>int] AS a, ROW(1, 2) AS b, LIST[[1,2],[3]] AS c
"""

cross_database_reference_schema_yml = """
version: 2

sources:
  - name: test_database_1
    database: test_database_1
    schema: public
    tables:
      - name: table1

  - name: test_database_2
    database: test_database_2
    schema: public
    tables:
      - name: table2

models:
    - name: cross_db_reference
      description: "A model that references tables from two different databases"
      columns:
          - name: id
            description: "The ID from test_database_1.table1 that matches test_database_2.table2"
"""

cross_database_reference_sql = """
{{ config(materialized='materialized_view', schema='public') }}
    WITH db1_data AS (
        SELECT id
        FROM {{ source('test_database_1', 'table1') }}
    ),
    db2_data AS (
        SELECT id
        FROM {{ source('test_database_2', 'table2') }}
    )
    SELECT db1_data.id
    FROM db1_data
    JOIN db2_data ON db1_data.id = db2_data.id
"""
