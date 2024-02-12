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

{% macro is_cluster_ready(cluster=target.cluster|default(none)) %}

-- Check if the cluster argument was provided or exists in the target profile
{% if cluster is none %}
    {{ exceptions.CompilationError("No cluster specified and no default cluster found in target profile. " ~ current_target_name) }}
{% endif %}

{{ log("Checking pending objects for cluster " ~ cluster, info=True) }}

{% set cluster_exists %}
WITH total_replicas AS (
    SELECT cluster_id, count(*) AS replicas
    FROM mz_cluster_replicas
    GROUP BY cluster_id
)

SELECT COALESCE(replicas, 0) AS replicas
FROM mz_clusters
LEFT JOIN total_replicas ON mz_clusters.id = cluster_id
WHERE mz_clusters.name = lower('{{ cluster }}')
{%- endset -%}

{%- set result = run_query(cluster_exists) %}
{%- if execute -%}
    {%- if result and result.rows|length > 0 -%}
        {%- set replicas_value = result.rows[0][0] %}
        {%- if replicas_value is none or replicas_value == 0 -%}
            {{ log("Cluster " ~ cluster ~ " has no running replicas", info=true) }}
            {{ return(false) }}
        {%- endif -%}
    {%- else -%}
        {{ log("Cluster " ~ cluster ~ " does not exist", info=true) }}
        {{ return(false) }}
    {%- endif -%}
{%- endif -%}

{%- set check_pending_objects_sql %}
WITH dataflows AS (
    SELECT mz_indexes.id, mz_indexes.name, 'index' AS type
    FROM mz_indexes
    JOIN mz_clusters ON mz_indexes.cluster_id = mz_clusters.id
    WHERE mz_clusters.name = lower('{{ cluster }}')

    UNION ALL

    SELECT mz_materialized_views.id, mz_materialized_views.name, 'materialized-view' AS type
    FROM mz_materialized_views
    JOIN mz_clusters ON mz_materialized_views.cluster_id = mz_clusters.id
    WHERE mz_clusters.name = lower('{{ cluster }}')
),

ready_dataflows AS (
    SELECT id, name, type
    FROM dataflows AS d
    JOIN mz_internal.mz_compute_hydration_statuses AS h ON (h.object_id = d.id)
    LEFT JOIN mz_internal.mz_materialization_lag AS l ON (l.object_id = d.id)
    WHERE h.hydrated AND (l.local_lag <= '1s' OR l.local_lag IS NULL)
),

pending_dataflows AS (
    SELECT id, name, type
    FROM dataflows d

    EXCEPT

    SELECT id, name, type
    FROM ready_dataflows r

),

pending_sources AS (
    SELECT mz_sources.id, mz_sources.name, 'source' AS type
    FROM mz_sources
    JOIN mz_clusters ON mz_clusters.id = mz_sources.cluster_id
    JOIN mz_internal.mz_source_statistics s ON mz_sources.id = s.id
    WHERE mz_clusters.name = lower('{{ cluster }}')
        AND snapshot_committed IS FALSE
),

pending_sinks AS (
    SELECT mz_sinks.id, mz_sinks.name, 'sink' AS type
    FROM mz_sinks
    JOIN mz_clusters ON mz_clusters.id = mz_sinks.cluster_id
    JOIN mz_internal.mz_sink_statuses s ON mz_sinks.id = s.id
    WHERE mz_clusters.name = lower('{{ cluster }}')
        AND status <> 'running'
),

pending_objects AS (
    SELECT *
    FROM pending_dataflows

    UNION ALL

    SELECT *
    FROM pending_sources

    UNION ALL

    SELECT *
    FROM pending_sinks
)

SELECT * FROM pending_objects
{%- endset %}

-- Execute the SQL and store the result
{%- set results = run_query(check_pending_objects_sql) %}
{%- if execute -%}
    {#- If there are results, the query will return at least one row -#}
    {%- if results and results.column_names and results.rows|length > 0 -%}
        {#- There are pending objects, so print them -#}
        {{ log("Pending objects found for cluster " ~ cluster ~ ":", info=True) }}
        {%- for row in results.rows -%}
            {{ log("- [" ~ row[2] ~ "(" ~ row[0] ~ ")]: " ~ row[1], info=True) }}
        {%- endfor -%}
        {{ return(false) }}
    {%- else -%}
        {#- No pending objects found for the specified cluster -#}
        {{ log("No pending objects found for cluster " ~ cluster ~ ". Cluster is ready.", info=True) }}
        {{ return(true) }}
    {%- endif -%}
{%- endif -%}

{% endmacro %}
