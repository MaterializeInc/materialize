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
  are_clusters_ready macro
  ========================

  Checks readiness of multiple clusters in a single query to reduce
  catalog server load. Instead of issuing one query per cluster per poll
  iteration, this macro consolidates all cluster checks into one round-trip.

  Evaluates:
  1. Replica health (OOM detection)
  2. Hydration status — are all objects hydrated?
  3. Lag check — is the cluster caught up within the allowed threshold?

  Returns a dictionary keyed by cluster name, where each value is a dict with:
  - ready: bool
  - status: str - 'ready', 'hydrating', 'lagging', or 'failing'
  - failure_reason: str or None
  - hydrated_count: int
  - total_count: int
  - max_lag_secs: int
  - total_replicas: int
  - problematic_replicas: int
#}

{% macro are_clusters_ready(clusters, lag_threshold='1s') %}

{% if clusters | length == 0 %}
    {{ return({}) }}
{% endif %}

{{ log("Checking cluster readiness for: " ~ clusters | join(", "), info=True) }}

{# Build the IN clause for all clusters #}
{% set cluster_list = [] %}
{% for cluster in clusters %}
    {% do cluster_list.append(dbt.string_literal(cluster)) %}
{% endfor %}
{% set in_clause = cluster_list | join(", ") %}

{%- set check_clusters_ready_sql %}
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
    WHERE c.name IN ({{ in_clause }})
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
    WHERE c.name IN ({{ in_clause }})
    GROUP BY c.name, r.id
),

hydration_best AS (
    SELECT cluster_name, MAX(hydrated) AS hydrated, MAX(total) AS total
    FROM hydration_counts
    GROUP BY cluster_name
),

-- Max lag per cluster
cluster_lag AS (
    SELECT
        c.name AS cluster_name,
        MAX(EXTRACT(EPOCH FROM wgl.lag)) AS max_lag_secs
    FROM mz_clusters c
    JOIN mz_cluster_replicas r ON c.id = r.cluster_id
    JOIN mz_internal.mz_hydration_statuses mhs ON mhs.replica_id = r.id
    JOIN mz_internal.mz_wallclock_global_lag wgl ON wgl.object_id = mhs.object_id
    WHERE c.name IN ({{ in_clause }})
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

{%- set results = run_query(check_clusters_ready_sql) %}
{%- set cluster_statuses = {} %}

{%- if execute -%}
    {%- if results and results.rows | length > 0 -%}
        {%- for row in results.rows -%}
            {%- set cluster_name = row[0] -%}
            {%- set cluster_id = row[1] -%}
            {%- set status = row[2] -%}
            {%- set failure_reason = row[3] -%}
            {%- set hydrated_count = row[4] -%}
            {%- set total_count = row[5] -%}
            {%- set max_lag_secs = row[6] -%}
            {%- set total_replicas = row[7] -%}
            {%- set problematic_replicas = row[8] -%}

            {%- if status == 'ready' -%}
                {{ log("  " ~ cluster_name ~ ": ready (hydration: " ~ hydrated_count ~ "/" ~ total_count ~ ", lag: " ~ max_lag_secs ~ "s)", info=True) }}
            {%- elif status == 'hydrating' -%}
                {{ log("  " ~ cluster_name ~ ": hydrating (" ~ hydrated_count ~ "/" ~ total_count ~ " objects)", info=True) }}
            {%- elif status == 'lagging' -%}
                {{ log("  " ~ cluster_name ~ ": lagging (" ~ max_lag_secs ~ "s, threshold: " ~ row[9] ~ "s)", info=True) }}
            {%- elif status == 'failing' -%}
                {{ log("  " ~ cluster_name ~ ": failing (" ~ failure_reason ~ ", " ~ problematic_replicas ~ "/" ~ total_replicas ~ " problematic replicas)", info=True) }}
            {%- endif -%}

            {%- do cluster_statuses.update({cluster_name: {
                'ready': status == 'ready',
                'status': status,
                'failure_reason': failure_reason,
                'hydrated_count': hydrated_count,
                'total_count': total_count,
                'max_lag_secs': max_lag_secs,
                'total_replicas': total_replicas,
                'problematic_replicas': problematic_replicas
            }}) -%}
        {%- endfor -%}
    {%- endif -%}

    {#- Mark any clusters not found in results as failing -#}
    {%- for cluster in clusters -%}
        {%- if cluster not in cluster_statuses -%}
            {{ log("  " ~ cluster ~ ": not found", info=True) }}
            {%- do cluster_statuses.update({cluster: {
                'ready': false,
                'status': 'failing',
                'failure_reason': 'cluster_not_found',
                'hydrated_count': 0,
                'total_count': 0,
                'max_lag_secs': 0,
                'total_replicas': 0,
                'problematic_replicas': 0
            }}) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

{{ return(cluster_statuses) }}

{% endmacro %}
