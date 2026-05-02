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

{#
  is_cluster_ready macro
  ======================

  Checks if a cluster is ready for use by evaluating:
  - Replica health (detects OOM-killed replicas)
  - Hydration status (are all objects hydrated?)
  - Lag (is the cluster caught up within the allowed threshold?)

  Returns a dictionary with:
  - ready: bool - True if status is 'ready'
  - status: str - 'ready', 'hydrating', 'lagging', or 'failing'
  - failure_reason: str - 'no_replicas', 'all_replicas_problematic', or None
  - hydrated_count: int - Number of hydrated objects
  - total_count: int - Total objects to hydrate
  - max_lag_secs: int - Maximum lag in seconds
  - total_replicas: int - Total replicas in cluster
  - problematic_replicas: int - Replicas with 3+ OOM kills in 24h
#}

{% macro is_cluster_ready(cluster=target.cluster|default(none), lag_threshold='10s') %}

{% if cluster is none %}
    {{ exceptions.raise_compiler_error("No cluster specified and no default cluster found in target profile.") }}
{% endif %}

{{ log("Checking cluster readiness for: " ~ cluster, info=True) }}

{%- set check_cluster_ready_sql %}
WITH
-- Detect problematic replicas: 3+ OOM kills in 24h
problematic_replicas AS (
    SELECT replica_id
    FROM mz_internal.mz_cluster_replica_status_history
    WHERE occurred_at + INTERVAL '24 hours' > mz_now()
      AND reason = 'oom-killed'
    GROUP BY replica_id
    HAVING COUNT(*) >= 3
),

-- Cluster health: count total vs problematic replicas
cluster_health AS (
    SELECT
        c.name AS cluster_name,
        c.id AS cluster_id,
        COUNT(r.id) AS total_replicas,
        COUNT(pr.replica_id) AS problematic_replicas
    FROM mz_clusters c
    LEFT JOIN mz_cluster_replicas r ON c.id = r.cluster_id
    LEFT JOIN problematic_replicas pr ON r.id = pr.replica_id
    WHERE c.name = {{ dbt.string_literal(cluster) }}
    GROUP BY c.name, c.id
),

-- Hydration counts per cluster (best replica)
hydration_counts AS (
    SELECT
        c.name AS cluster_name,
        r.id AS replica_id,
        COUNT(*) FILTER (WHERE mhs.hydrated) AS hydrated,
        COUNT(*) AS total
    FROM mz_clusters c
    JOIN mz_cluster_replicas r ON c.id = r.cluster_id
    LEFT JOIN mz_internal.mz_hydration_statuses mhs ON mhs.replica_id = r.id
    WHERE c.name = {{ dbt.string_literal(cluster) }}
    GROUP BY c.name, r.id
),

hydration_best AS (
    SELECT cluster_name, MAX(hydrated) AS hydrated, MAX(total) AS total
    FROM hydration_counts
    GROUP BY cluster_name
),

-- Max lag per cluster using mz_wallclock_global_lag
cluster_lag AS (
    SELECT
        c.name AS cluster_name,
        MAX(EXTRACT(EPOCH FROM wgl.lag)) AS max_lag_secs
    FROM mz_clusters c
    JOIN mz_cluster_replicas r ON c.id = r.cluster_id
    JOIN mz_internal.mz_hydration_statuses mhs ON mhs.replica_id = r.id
    JOIN mz_internal.mz_wallclock_global_lag wgl ON wgl.object_id = mhs.object_id
    WHERE c.name = {{ dbt.string_literal(cluster) }}
    GROUP BY c.name
),

-- Convert lag_threshold interval to seconds
lag_threshold_secs AS (
    SELECT EXTRACT(EPOCH FROM INTERVAL {{ dbt.string_literal(lag_threshold) }}) AS threshold_secs
)

SELECT
    ch.cluster_name,
    ch.cluster_id,
    CASE
        WHEN ch.total_replicas = 0 THEN 'failing'
        WHEN ch.total_replicas = ch.problematic_replicas THEN 'failing'
        WHEN COALESCE(hb.hydrated, 0) < COALESCE(hb.total, 0) THEN 'hydrating'
        WHEN COALESCE(cl.max_lag_secs, 0) > (SELECT threshold_secs FROM lag_threshold_secs) THEN 'lagging'
        ELSE 'ready'
    END AS status,
    CASE
        WHEN ch.total_replicas = 0 THEN 'no_replicas'
        WHEN ch.total_replicas = ch.problematic_replicas THEN 'all_replicas_problematic'
        ELSE NULL
    END AS failure_reason,
    COALESCE(hb.hydrated, 0) AS hydrated_count,
    COALESCE(hb.total, 0) AS total_count,
    COALESCE(cl.max_lag_secs, 0)::bigint AS max_lag_secs,
    ch.total_replicas,
    ch.problematic_replicas,
    (SELECT threshold_secs FROM lag_threshold_secs)::bigint AS threshold_secs
FROM cluster_health ch
LEFT JOIN hydration_best hb ON ch.cluster_name = hb.cluster_name
LEFT JOIN cluster_lag cl ON ch.cluster_name = cl.cluster_name
{%- endset %}

{%- set results = run_query(check_cluster_ready_sql) %}
{%- if execute -%}
    {%- if results and results.rows|length > 0 -%}
        {%- set row = results.rows[0] -%}
        {%- set cluster_name = row[0] -%}
        {%- set cluster_id = row[1] -%}
        {%- set status = row[2] -%}
        {%- set failure_reason = row[3] -%}
        {%- set hydrated_count = row[4] -%}
        {%- set total_count = row[5] -%}
        {%- set max_lag_secs = row[6] -%}
        {%- set total_replicas = row[7] -%}
        {%- set problematic_replicas = row[8] -%}
        {%- set threshold_secs = row[9] -%}

        {#- Log status details -#}
        {%- if status == 'ready' -%}
            {{ log("Cluster " ~ cluster ~ " is ready. Hydration: " ~ hydrated_count ~ "/" ~ total_count ~ ", Lag: " ~ max_lag_secs ~ "s", info=True) }}
        {%- elif status == 'hydrating' -%}
            {{ log("Cluster " ~ cluster ~ " is hydrating: " ~ hydrated_count ~ "/" ~ total_count ~ " objects hydrated", info=True) }}
        {%- elif status == 'lagging' -%}
            {{ log("Cluster " ~ cluster ~ " is lagging: " ~ max_lag_secs ~ "s (threshold: " ~ threshold_secs ~ "s)", info=True) }}
        {%- elif status == 'failing' -%}
            {{ log("Cluster " ~ cluster ~ " is failing: " ~ failure_reason ~ " (" ~ problematic_replicas ~ "/" ~ total_replicas ~ " problematic replicas)", info=True) }}
        {%- endif -%}

        {{ return({
            'ready': status == 'ready',
            'status': status,
            'failure_reason': failure_reason,
            'hydrated_count': hydrated_count,
            'total_count': total_count,
            'max_lag_secs': max_lag_secs,
            'total_replicas': total_replicas,
            'problematic_replicas': problematic_replicas
        }) }}
    {%- else -%}
        {{ log("Cluster " ~ cluster ~ " does not exist", info=True) }}
        {{ return({
            'ready': false,
            'status': 'failing',
            'failure_reason': 'cluster_not_found',
            'hydrated_count': 0,
            'total_count': 0,
            'max_lag_secs': 0,
            'total_replicas': 0,
            'problematic_replicas': 0
        }) }}
    {%- endif -%}
{%- endif -%}

{% endmacro %}
