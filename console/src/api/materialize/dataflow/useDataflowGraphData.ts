// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";
import React from "react";

import { escapedLiteral as lit, useSqlManyTyped } from "~/api/materialize";
import { useLastGoodByKey } from "~/api/materialize/useLastGoodByKey";
import {
  buildDataflowStructure,
  type ChannelRow,
  type LirSpanRow,
  type OperatorRow,
  type PerWorkerStatRow,
} from "~/platform/dataflows/dataflowGraph";

import { queryBuilder } from "../db";

export interface DataflowGraphParams {
  clusterName: string;
  replicaName: string;
  dataflowId: string;
}

interface ReplicaWorkerCountRow {
  workerCount: bigint | number | string;
}

// A malformed id can only reach this hook via a hand-edited URL (the route
// itself only ever supplies real dataflow ids), so this returns undefined
// for the caller to render as a not-found state, rather than throwing
// inside useMemo and crashing to the error boundary over a typo in the
// address bar.
function dataflowIdLiteral(dataflowId: string) {
  if (!/^\d+$/.test(dataflowId)) return undefined;
  return sql`${lit(dataflowId)}::uint8`;
}

export function useDataflowGraphData(params?: DataflowGraphParams) {
  // Read primitives so an inline params object from a caller does not
  // recompile the queries (and refetch) on every render. Only the dataflow id
  // is interpolated into the SQL. Cluster and replica reach the four
  // dataflow-scoped queries below through the request options, so their text
  // doesn't depend on them; replicaWorkers is the one exception, since
  // mz_cluster_replicas/mz_cluster_replica_sizes are plain catalog tables,
  // not per-replica-scoped introspection relations that the session context
  // resolves automatically.
  const dataflowId = params?.dataflowId;
  const clusterName = params?.clusterName;
  const replicaName = params?.replicaName;
  const queries = React.useMemo(() => {
    if (
      dataflowId === undefined ||
      clusterName === undefined ||
      replicaName === undefined
    )
      return null;
    const id = dataflowIdLiteral(dataflowId);
    if (id === undefined) return null;
    return {
      operators: sql`
        SELECT
          mdod.id,
          mda.address,
          mdod.name,
          coalesce(mas.records, 0) AS "arrangementRecords",
          coalesce(mas.size, 0) AS "arrangementSize",
          coalesce(mse.elapsed_ns, 0) AS "elapsedNs",
          coalesce(mcodh.count, 0) AS "scheduleCount"
        FROM mz_dataflow_operator_dataflows AS mdod
        JOIN mz_dataflow_addresses AS mda ON mda.id = mdod.id
        LEFT JOIN mz_arrangement_sizes AS mas ON mas.operator_id = mdod.id
        LEFT JOIN mz_scheduling_elapsed AS mse ON mse.id = mdod.id
        LEFT JOIN (
          SELECT id, sum(count) AS count
          FROM mz_compute_operator_durations_histogram
          GROUP BY id
        ) AS mcodh ON mcodh.id = mdod.id
        WHERE mdod.dataflow_id = ${id}`
        .$castTo<OperatorRow>()
        .compile(queryBuilder),
      channels: sql`
        SELECT
          mdco.id,
          from_operator_address AS "fromOperatorAddress",
          mdc.from_port AS "fromPort",
          to_operator_address AS "toOperatorAddress",
          mdc.to_port AS "toPort",
          COALESCE(sum(mmc.sent), 0) AS "messagesSent",
          COALESCE(sum(mmc.batch_sent), 0) AS "batchesSent",
          mdco.type AS "channelType"
        FROM mz_dataflow_channel_operators AS mdco
        JOIN mz_dataflow_channels AS mdc ON mdc.id = mdco.id
        LEFT JOIN mz_message_counts AS mmc ON mdco.id = mmc.channel_id
        WHERE from_operator_address[1] = ${id}
        GROUP BY
          mdco.id, "fromOperatorAddress", mdc.from_port,
          "toOperatorAddress", mdc.to_port, mdco.type`
        .$castTo<ChannelRow>()
        .compile(queryBuilder),
      // mz_lir_mapping is keyed by the built object's global id (often a
      // transient id), which mz_compute_exports does not bridge to a dataflow.
      // Operator id ranges do: a span belongs to this dataflow when one of the
      // dataflow's operators falls inside its [start, end) range.
      lirSpans: sql`
        SELECT DISTINCT
          mlm.global_id AS "exportId",
          mlm.lir_id::text AS "lirId",
          mlm.parent_lir_id::text AS "parentLirId",
          mlm.nesting::int4 AS nesting,
          mlm.operator,
          mlm.operator_id_start AS "operatorIdStart",
          mlm.operator_id_end AS "operatorIdEnd"
        FROM mz_lir_mapping AS mlm
        WHERE EXISTS (
          SELECT 1
          FROM mz_dataflow_operator_dataflows AS dod
          WHERE dod.dataflow_id = ${id}
            AND dod.id >= mlm.operator_id_start
            AND dod.id < mlm.operator_id_end
        )`
        .$castTo<LirSpanRow>()
        .compile(queryBuilder),
      // Per-worker CPU/memory/schedule count, for the skew heatmap (worst
      // worker over the average). Three independent per-worker sources
      // full-outer-joined on (id, worker_id): an operator can show up in
      // one and not the others (e.g. it schedules but never arranges
      // anything), and a worker with no row in a source never touched that
      // side of it at all, which matters for skew (see dataflowGraph.ts)
      // and must not be coalesced to a false zero here.
      perWorkerStats: sql`
        WITH cpu AS (
          SELECT mdod.id, mse.worker_id, mse.elapsed_ns
          FROM mz_dataflow_operator_dataflows AS mdod
          JOIN mz_scheduling_elapsed_per_worker AS mse ON mse.id = mdod.id
          WHERE mdod.dataflow_id = ${id}
        ),
        memory AS (
          SELECT mdod.id, mas.worker_id, mas.size
          FROM mz_dataflow_operator_dataflows AS mdod
          JOIN mz_arrangement_sizes_per_worker AS mas ON mas.operator_id = mdod.id
          WHERE mdod.dataflow_id = ${id}
        ),
        schedules AS (
          SELECT mdod.id, mcodhpw.worker_id, sum(mcodhpw.count) AS count
          FROM mz_dataflow_operator_dataflows AS mdod
          JOIN mz_compute_operator_durations_histogram_per_worker AS mcodhpw
            ON mcodhpw.id = mdod.id
          WHERE mdod.dataflow_id = ${id}
          GROUP BY mdod.id, mcodhpw.worker_id
        )
        SELECT
          coalesce(cpu.id, memory.id, schedules.id) AS id,
          coalesce(cpu.worker_id, memory.worker_id, schedules.worker_id)
            AS "workerId",
          cpu.elapsed_ns AS "elapsedNs",
          memory.size AS "arrangementSize",
          schedules.count AS "scheduleCount"
        FROM cpu
        FULL OUTER JOIN memory
          ON memory.id = cpu.id AND memory.worker_id = cpu.worker_id
        FULL OUTER JOIN schedules
          ON schedules.id = coalesce(cpu.id, memory.id)
          AND schedules.worker_id = coalesce(cpu.worker_id, memory.worker_id)`
        .$castTo<PerWorkerStatRow>()
        .compile(queryBuilder),
      // The replica's total worker count (workers per process times
      // processes), the fixed ceiling the skew heatmap normalizes against
      // (see decorateGraph). Plain catalog tables, not introspection, so
      // this is the one query here that needs cluster/replica in its text
      // rather than relying on the request's session context.
      replicaWorkers: sql`
        SELECT crs.workers * crs.processes AS "workerCount"
        FROM mz_cluster_replicas AS cr
        JOIN mz_clusters AS c ON c.id = cr.cluster_id
        JOIN mz_cluster_replica_sizes AS crs ON crs.size = cr.size
        WHERE c.name = ${lit(clusterName)} AND cr.name = ${lit(replicaName)}`
        .$castTo<ReplicaWorkerCountRow>()
        .compile(queryBuilder),
    };
  }, [dataflowId, clusterName, replicaName]);

  const {
    results,
    error: queryError,
    databaseError,
    loading: queryLoading,
    refetch,
  } = useSqlManyTyped(queries, {
    cluster: params?.clusterName,
    replica: params?.replicaName,
    // This query can be slow for large dataflows.
    timeout: 30_000,
  });

  // A malformed dataflow id (only reachable via a hand-edited URL) never
  // compiles a query, so `loading` would otherwise stay false forever with
  // no error to show. Surfacing it here, rather than throwing inside the
  // queries useMemo above, lets the page render a real error message
  // instead of crashing to the error boundary over a URL typo.
  const invalidDataflowId =
    dataflowId !== undefined && dataflowIdLiteral(dataflowId) === undefined;
  const error = invalidDataflowId
    ? `invalid dataflow id: ${dataflowId}`
    : queryError;
  const loading = !invalidDataflowId && queryLoading;

  // JSON tuple, not a delimiter-joined string. Cluster and replica names are
  // identifiers that can themselves contain any delimiter we might pick.
  const key = params
    ? JSON.stringify([
        params.clusterName,
        params.replicaName,
        params.dataflowId,
      ])
    : null;

  const compute = React.useCallback(
    (r: NonNullable<typeof results>) => ({
      structure: buildDataflowStructure(
        r.operators ?? [],
        r.channels ?? [],
        r.lirSpans ?? [],
        r.perWorkerStats ?? [],
      ),
      // A replica always has at least 1 worker; falling back to 1 (a heatmap
      // ceiling of log2(1) = 0, disabling skew coloring entirely) only
      // matters if the query somehow returns no row.
      workerCount: Number(r.replicaWorkers?.[0]?.workerCount ?? 1),
      fetchedAt: new Date(),
    }),
    [],
  );
  const data = useLastGoodByKey({
    key,
    results,
    error,
    loading,
    compute,
  });

  return { data, error, databaseError, loading, refetch };
}
