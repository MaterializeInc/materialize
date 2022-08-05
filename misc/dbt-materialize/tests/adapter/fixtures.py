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

test_materialized_view = """
{{ config(materialized='materializedview') }}

    SELECT * FROM (VALUES ('chicken', 'pig'), ('cow', 'horse'), (NULL, NULL)) _ (a, b)
"""

test_materialized_view_index = """
{{ config(
    materialized='materializedview',
    indexes=[{'columns': ['a']}]
) }}

    SELECT * FROM (VALUES ('chicken', 'pig'), ('cow', 'horse')) _ (a, b)
"""

test_view_index = """
{{ config(
    materialized='view',
    indexes=[{'columns': ['a', 'length(a)']}]
) }}

    SELECT * FROM (VALUES ('chicken', 'pig'), ('cow', 'horse'), (NULL, NULL)) _ (a, b)
"""

test_source = """
{{ config(materialized='source', database='materialize') }}

    CREATE SOURCE {{ this }}
    FROM KAFKA BROKER '{{ env_var('KAFKA_ADDR', 'localhost:9092') }}' TOPIC 'test-source'
    FORMAT BYTES
"""

test_index = """
{{ config(materialized='index') }}

    CREATE DEFAULT INDEX test_index
    ON {{ ref('test_source') }}
"""

test_source_index = """
{{ config(
    materialized='source',
    indexes=[{'columns': ['data']}]
) }}

    CREATE SOURCE {{ mz_generate_name('test_source_index') }}
    FROM KAFKA BROKER '{{ env_var('KAFKA_ADDR', 'localhost:9092') }}' TOPIC 'test-source-index'
    FORMAT BYTES
"""

test_sink = """
{{ config(materialized='sink') }}

    CREATE SINK {{ this }}
    FROM {{ ref('test_materialized_view') }}
    INTO KAFKA BROKER '{{ env_var('KAFKA_ADDR', 'localhost:9092') }}' TOPIC 'test-sink'
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '{{ env_var('SCHEMA_REGISTRY_URL', 'http://localhost:8081') }}'
"""

actual_indexes = """
SELECT
    o.name,
    ic.index_position,
    ic.on_position,
    ic.on_expression
FROM mz_indexes i
JOIN mz_index_columns ic ON i.id = ic.index_id
JOIN mz_objects o ON i.on_id = o.id
WHERE i.id LIKE 'u%'
"""

expected_indexes = """
name,index_position,on_position,on_expression
test_materialized_view_index,1,1,
test_source,1,1,
test_source_index,1,1,
test_view_index,1,1,
test_view_index,2,,pg_catalog.length(a)
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
