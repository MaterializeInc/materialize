// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/*
 * Finds blue/green deploys from the system catalog alone, so the page works for
 * mz-deploy, dbt and hand-run deploys without any setup.
 *
 * Every blue/green tool stages a schema or cluster `X` as `X_<suffix>`, then
 * promotes with `ALTER ... SWAP`, which the audit log records as a rename of
 * the staged object from `X_<suffix>` to `X`. A staged object that is dropped
 * without that rename was aborted.
 */

import { QueryKey } from "@tanstack/react-query";
import { sql } from "kysely";

import { executeSqlV2, queryBuilder } from "~/api/materialize";
import {
  DeployEventRow,
  StagedObjectRow,
} from "~/platform/deployments/deploymentModel";

/**
 * Schemas and clusters staged now: named `<live name>_<suffix>` next to a live
 * one. A staged schema must share an object name with its live schema, which
 * keeps unrelated pairs such as `sales` and `sales_eu` out. A staged cluster
 * must share its suffix with a staged schema.
 *
 * An object renamed by a swap is the retired live copy (dbt keeps it until
 * `deploy_cleanup`), not a staged one.
 */
export function buildStagedQuery() {
  return sql<StagedObjectRow>`
    WITH
    sch AS (
      SELECT s.id, s.name, s.database_id, d.name AS database
      FROM mz_catalog.mz_schemas s
      JOIN mz_catalog.mz_databases d ON d.id = s.database_id
      WHERE s.id LIKE 'u%' AND d.name <> '_mz_deploy'
    ),
    -- Schema and cluster IDs are separate namespaces, so match on both.
    swapped AS (
      SELECT DISTINCT object_type, details->>'id' AS id
      FROM mz_catalog.mz_audit_events
      WHERE event_type = 'alter'
        AND object_type IN ('schema', 'cluster')
        AND details->>'old_name' IS NOT NULL
    ),
    created AS (
      SELECT details->>'id' AS id, object_type, occurred_at, "user"
      FROM mz_catalog.mz_audit_events
      WHERE event_type = 'create' AND object_type IN ('schema', 'cluster')
    ),
    schema_pairs AS (
      SELECT st.id, st.database, live.name AS live_name, st.name AS staged_name,
        substr(st.name, length(live.name) + 2) AS suffix,
        length(live.name) AS live_length
      FROM sch st
      JOIN sch live ON live.database_id = st.database_id
        AND length(st.name) > length(live.name) + 1
        AND left(st.name, length(live.name) + 1) = live.name || '_'
      WHERE NOT EXISTS (
          SELECT 1 FROM swapped w WHERE w.object_type = 'schema' AND w.id = st.id
        )
        AND EXISTS (
          SELECT 1
          FROM mz_catalog.mz_objects a
          JOIN mz_catalog.mz_objects b ON b.name = a.name
          WHERE a.schema_id = st.id AND b.schema_id = live.id
        )
    ),
    -- Of the live schemas a staged name extends (\`a\` and \`a_b\` for
    -- \`a_b_123\`), the longest is the one it copies.
    staged_schemas AS (
      SELECT * FROM schema_pairs p
      WHERE live_length =
        (SELECT max(live_length) FROM schema_pairs q WHERE q.id = p.id)
    ),
    staged_clusters AS (
      SELECT st.id, NULL::text AS database, live.name AS live_name,
        st.name AS staged_name,
        substr(st.name, length(live.name) + 2) AS suffix
      FROM mz_catalog.mz_clusters st
      JOIN mz_catalog.mz_clusters live
        ON length(st.name) > length(live.name) + 1
        AND left(st.name, length(live.name) + 1) = live.name || '_'
      WHERE st.id LIKE 'u%'
        AND NOT EXISTS (
          SELECT 1 FROM swapped w
          WHERE w.object_type = 'cluster' AND w.id = st.id
        )
        AND substr(st.name, length(live.name) + 2)
          IN (SELECT suffix FROM staged_schemas)
    ),
    staged AS (
      SELECT 'schema' AS kind, id, database, live_name, staged_name, suffix
      FROM staged_schemas
      UNION ALL
      SELECT 'cluster', id, database, live_name, staged_name, suffix
      FROM staged_clusters
    )
    SELECT
      staged.kind,
      staged.id,
      staged.database,
      staged.live_name AS "liveName",
      staged.staged_name AS "stagedName",
      staged.suffix,
      created.occurred_at AS "stagedAt",
      created."user" AS "stagedBy"
    FROM staged
    LEFT JOIN created
      ON created.id = staged.id AND created.object_type = staged.kind`.compile(
    queryBuilder,
  );
}

/**
 * Past deploys from the audit log: promotes (a rename from `X_<suffix>` to
 * `X`) and aborts (an object created as `X_<suffix>`, next to a live `X`, then
 * dropped without being renamed).
 *
 * NOTE: an abort is recognized by name only, because a dropped schema's
 * objects are gone. A schema named like a live one plus a suffix that was
 * created and dropped for another reason also shows as aborted.
 */
export function buildHistoryQuery() {
  return sql<DeployEventRow>`
    WITH
    ev AS (
      SELECT event_type, object_type, "user", occurred_at,
        details->>'id' AS id,
        details->>'database_name' AS database,
        details->>'name' AS name,
        details->>'old_name' AS old_name,
        details->>'new_name' AS new_name
      FROM mz_catalog.mz_audit_events
      WHERE object_type IN ('schema', 'cluster')
        AND event_type IN ('create', 'alter', 'drop')
        AND coalesce(details->>'database_name', '') <> '_mz_deploy'
    ),
    created AS (SELECT * FROM ev WHERE event_type = 'create'),
    renamed AS (
      SELECT DISTINCT object_type, id FROM ev
      WHERE event_type = 'alter' AND old_name IS NOT NULL
    ),
    live AS (
      SELECT 'schema' AS kind, s.name, d.name AS database
      FROM mz_catalog.mz_schemas s
      JOIN mz_catalog.mz_databases d ON d.id = s.database_id
      UNION ALL
      SELECT 'cluster', name, NULL FROM mz_catalog.mz_clusters
    ),
    promotes AS (
      SELECT 'promoted' AS state, e.object_type AS kind, e.id, e.database,
        e.new_name AS live_name, e.old_name AS staged_name,
        substr(e.old_name, length(e.new_name) + 2) AS suffix,
        c.occurred_at AS staged_at, c."user" AS staged_by,
        e.occurred_at AS ended_at, e."user" AS ended_by
      FROM ev e
      LEFT JOIN created c ON c.id = e.id AND c.object_type = e.object_type
      WHERE e.event_type = 'alter'
        AND length(e.old_name) > length(e.new_name) + 1
        AND left(e.old_name, length(e.new_name) + 1) = e.new_name || '_'
    ),
    aborts AS (
      SELECT 'aborted' AS state, c.object_type AS kind, c.id, c.database,
        live.name AS live_name, c.name AS staged_name,
        substr(c.name, length(live.name) + 2) AS suffix,
        c.occurred_at AS staged_at, c."user" AS staged_by,
        d.occurred_at AS ended_at, d."user" AS ended_by
      FROM created c
      JOIN ev d ON d.event_type = 'drop'
        AND d.id = c.id AND d.object_type = c.object_type
      JOIN live ON live.kind = c.object_type
        AND live.database IS NOT DISTINCT FROM c.database
        AND length(c.name) > length(live.name) + 1
        AND left(c.name, length(live.name) + 1) = live.name || '_'
      WHERE NOT EXISTS (
        SELECT 1 FROM renamed r
        WHERE r.object_type = c.object_type AND r.id = c.id
      )
    ),
    events AS (SELECT * FROM promotes UNION ALL SELECT * FROM aborts)
    SELECT
      state,
      kind,
      id,
      database,
      live_name AS "liveName",
      staged_name AS "stagedName",
      suffix,
      staged_at AS "stagedAt",
      staged_by AS "stagedBy",
      ended_at AS "endedAt",
      ended_by AS "endedBy"
    FROM events`.compile(queryBuilder);
}

/** mz-deploy keeps its tracking tables here, which tells its deploys apart. */
export function buildMzDeployPresentQuery() {
  return sql<{ present: boolean }>`
    SELECT EXISTS (
      SELECT 1 FROM mz_catalog.mz_databases WHERE name = '_mz_deploy'
    ) AS present`.compile(queryBuilder);
}

/** What is staged now, and whether mz-deploy is set up. */
export async function fetchStaged({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const [staged, mzDeploy] = await executeSqlV2({
    queries: [buildStagedQuery(), buildMzDeployPresentQuery()] as const,
    queryKey,
    requestOptions,
  });
  return {
    staged: staged.rows,
    hasMzDeploy: mzDeploy.rows[0]?.present ?? false,
  };
}

export async function fetchHistory({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const { rows } = await executeSqlV2({
    queries: buildHistoryQuery(),
    queryKey,
    requestOptions,
  });
  return rows;
}
