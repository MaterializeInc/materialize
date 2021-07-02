-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License in the LICENSE file at the
-- root of this repository, or online at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

{% set source_name %}
    {{ mz_generate_name('market_orders_raw') }}
{% endset %}

{% set source_stmt %}
    CREATE MATERIALIZED SOURCE {{ source_name }} FROM PUBNUB
    SUBSCRIBE KEY 'sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe'
    CHANNEL 'pubnub-market-orders';
{% endset %}

--- DROP SOURCE, CREATE SOURCE
{{ mz_drop_source(source_name, cascade=True) }}
{{ mz_create_source(source_stmt) }}

--- CREATE INDEX ON UNMATERIALIZED SOURCE, DROP THEM
{{ mz_drop_index(source_name, default=True, cascade=True) }}
{{ mz_create_index(source_name, default=True) }}
{{ mz_drop_index("non_default_idx", cascade=True) }}
{{ mz_create_index(source_name, idx_name="non_default_idx", col_refs=["text"], with_options=["logical_compaction_window = '500ms'"])}}

--- CREATE MATERIALIZED VIEW
{{ config(materialized='materializedview') }}

SELECT
    val->>'symbol' AS symbol,
    (val->'bid_price')::float AS bid_price
FROM (SELECT text::jsonb AS val FROM {{ source_name }})

--- CREATE SINK
-- {% set sink_name %}
--     {{ mz_generate_name('market_orders_sink') }}
-- {% endset %}

-- {% set sink_stmt %}
--     CREATE SINK {{ sink_name }}
--     FROM "market_orders"
--     INTO AVRO OCF '/tester.ocf;'
-- {% endset %}

-- {{ mz_drop_sink(sink_name) }}
-- {{ mz_create_sink(sink_stmt) }}
