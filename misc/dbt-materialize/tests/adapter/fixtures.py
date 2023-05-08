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
{{ config(materialized='materializedview') }}

    SELECT * FROM (VALUES ('chicken', 'pig'), ('cow', 'horse'), (NULL, NULL)) _ (a, b)
"""

test_materialized_view_index = """
{{ config(
    materialized='materializedview',
    indexes=[{'columns': ['a', 'length(a)'], 'name': 'a_idx'}]
) }}

    SELECT * FROM (VALUES ('chicken', 'pig'), ('cow', 'horse')) _ (a, b)
"""

test_view_index = """
{{ config(
    materialized='view',
    indexes=[{'default': True}]
) }}

    SELECT * FROM (VALUES ('chicken', 'pig'), ('cow', 'horse'), (NULL, NULL)) _ (a, b)
"""

test_source = """
{{ config(
    materialized='source',
    database='materialize',
    pre_hook="CREATE CONNECTION kafka_connection TO KAFKA (BROKER '{{ env_var('KAFKA_ADDR', 'localhost:9092') }}')"
    )
}}

CREATE SOURCE {{ this }}
FROM KAFKA CONNECTION kafka_connection (TOPIC 'test-source')
FORMAT BYTES
"""

test_source_index = """
{{ config(
    materialized='source',
    indexes=[{'columns': ['data']}]
) }}

CREATE SOURCE {{ this }}
FROM KAFKA CONNECTION kafka_connection (TOPIC 'test-source')
FORMAT BYTES
"""

test_sink = """
{{ config(materialized='sink') }}
 CREATE SINK {{ this }}
 FROM {{ ref('test_materialized_view') }}
 INTO KAFKA CONNECTION kafka_connection (TOPIC 'test-sink')
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
    "base": "materializedview",
    "view_model": "view",
    "table_model": "materializedview",
    "swappable": "materializedview",
}

test_relation_name_length = """
{{ config(materialized='materializedview') }}

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
