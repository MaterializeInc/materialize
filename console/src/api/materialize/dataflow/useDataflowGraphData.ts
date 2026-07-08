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
import {
  buildDataflowStructure,
  type ChannelRow,
  type DataflowStructure,
  type LirSpanRow,
  type OperatorRow,
} from "~/platform/dataflows/dataflowGraph";

import { queryBuilder } from "../db";

export interface DataflowGraphParams {
  clusterName: string;
  replicaName: string;
  dataflowId: string;
}

function dataflowIdLiteral(dataflowId: string) {
  if (!/^\d+$/.test(dataflowId)) {
    throw new Error(`invalid dataflow id: ${dataflowId}`);
  }
  return sql`${lit(dataflowId)}::uint8`;
}

export function useDataflowGraphData(params?: DataflowGraphParams) {
  // Read primitives so an inline params object from a caller does not
  // recompile the queries (and refetch) on every render. Only the dataflow id
  // is interpolated into the SQL. Cluster and replica reach the request
  // through the options below, so the query text does not depend on them.
  const dataflowId = params?.dataflowId;
  const queries = React.useMemo(() => {
    if (dataflowId === undefined) return null;
    const id = dataflowIdLiteral(dataflowId);
    return {
      operators: sql`
        SELECT
          mdod.id,
          mda.address,
          mdod.name,
          coalesce(mas.records, 0) AS "arrangementRecords",
          coalesce(mas.size, 0) AS "arrangementSize",
          coalesce(mse.elapsed_ns, 0) AS "elapsedNs"
        FROM mz_dataflow_operator_dataflows AS mdod
        JOIN mz_dataflow_addresses AS mda ON mda.id = mdod.id
        LEFT JOIN mz_arrangement_sizes AS mas ON mas.operator_id = mdod.id
        LEFT JOIN mz_scheduling_elapsed AS mse ON mse.id = mdod.id
        WHERE mdod.dataflow_id = ${id}`
        .$castTo<OperatorRow>()
        .compile(queryBuilder),
      channels: sql`
        SELECT
          mdco.id,
          from_operator_address AS "fromOperatorAddress",
          from_port AS "fromPort",
          to_operator_address AS "toOperatorAddress",
          to_port AS "toPort",
          COALESCE(sum(mmc.sent), 0) AS "messagesSent",
          COALESCE(sum(mmc.batch_sent), 0) AS "batchesSent",
          mdco.type AS "channelType"
        FROM mz_dataflow_channel_operators AS mdco
        LEFT JOIN mz_message_counts AS mmc ON mdco.id = mmc.channel_id
        WHERE from_operator_address[1] = ${id}
        GROUP BY
          mdco.id, "fromOperatorAddress", "fromPort",
          "toOperatorAddress", "toPort", mdco.type`
        .$castTo<ChannelRow>()
        .compile(queryBuilder),
      lirSpans: sql`
        SELECT
          mce.export_id AS "exportId",
          mlm.lir_id::text AS "lirId",
          mlm.operator,
          mlm.operator_id_start AS "operatorIdStart",
          mlm.operator_id_end AS "operatorIdEnd"
        FROM mz_lir_mapping AS mlm
        JOIN mz_compute_exports AS mce ON mlm.global_id = mce.export_id
        WHERE mce.dataflow_id = ${id}`
        .$castTo<LirSpanRow>()
        .compile(queryBuilder),
    };
  }, [dataflowId]);

  const { results, error, databaseError, loading, refetch } = useSqlManyTyped(
    queries,
    {
      cluster: params?.clusterName,
      replica: params?.replicaName,
      // This query can be slow for large dataflows.
      timeout: 30_000,
    },
  );

  // JSON tuple, not a delimiter-joined string. Cluster and replica names are
  // identifiers that can themselves contain any delimiter we might pick.
  const key = params
    ? JSON.stringify([
        params.clusterName,
        params.replicaName,
        params.dataflowId,
      ])
    : null;
  const [lastGood, setLastGood] = React.useState<{
    key: string;
    data: { structure: DataflowStructure; fetchedAt: Date };
  } | null>(null);

  // Whether a fetch has actually started under the current key. Guards against
  // tagging the previous fetch's still-resident results with the new key in
  // the render after the selection changes but before useSqlMany's runSql
  // effect flips loading. Without it the old graph would show under the new
  // selection until the new fetch resolves (CNS-109).
  const sawLoadingRef = React.useRef(false);

  // Reset acceptance synchronously when the selection changes. Using the
  // render-phase state-adjustment pattern so the reset lands before any effect
  // runs, not one render later.
  const [trackedKey, setTrackedKey] = React.useState(key);
  if (key !== trackedKey) {
    setTrackedKey(key);
    sawLoadingRef.current = false;
  }

  // Tag successful results with the key they were fetched for, but only once a
  // fetch has started under that key.
  React.useEffect(() => {
    if (loading) sawLoadingRef.current = true;
    if (key && results && !error && !loading && sawLoadingRef.current) {
      setLastGood({
        key,
        data: {
          structure: buildDataflowStructure(
            results.operators ?? [],
            results.channels ?? [],
            results.lirSpans ?? [],
          ),
          fetchedAt: new Date(),
        },
      });
    }
  }, [key, results, error, loading]);

  // Error always wins, and data for other params never renders.
  const data =
    !error && lastGood && lastGood.key === key ? lastGood.data : null;
  return { data, error, databaseError, loading, refetch };
}
