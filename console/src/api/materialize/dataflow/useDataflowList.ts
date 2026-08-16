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

import { useSqlManyTyped } from "~/api/materialize";
import { useLastGoodByKey } from "~/api/materialize/useLastGoodByKey";

import { queryBuilder } from "../db";

export interface DataflowListParams {
  clusterName: string;
  replicaName: string;
}

export interface DataflowListEntry {
  id: string; // uint8 as string
  name: string;
  records: bigint;
  size: bigint;
  elapsedNs: bigint;
}

// Raw row shape as returned by the query. The numeric columns arrive as
// strings and are widened to bigint in normalize.
interface DataflowListRow {
  id: string;
  name: string;
  records: string;
  size: string;
  elapsedNs: string;
}

function normalize(rows: DataflowListRow[]): DataflowListEntry[] {
  return rows.map((row) => ({
    id: row.id,
    name: row.name,
    records: BigInt(row.records),
    size: BigInt(row.size),
    elapsedNs: BigInt(row.elapsedNs),
  }));
}

export function useDataflowList(params?: DataflowListParams) {
  // The query text is constant. Cluster and replica reach the request through
  // the options below, so recompiling only depends on whether params exist.
  const enabled = params !== undefined;
  const queries = React.useMemo(() => {
    if (!enabled) return null;
    return {
      list: sql`
        SELECT
          md.id::text AS id,
          md.name,
          COALESCE(mrpd.records, 0) AS records,
          COALESCE(mrpd.size, 0) AS size,
          COALESCE(el.elapsed_ns, 0) AS "elapsedNs"
        FROM mz_dataflows AS md
        LEFT JOIN mz_records_per_dataflow AS mrpd ON mrpd.id = md.id
        LEFT JOIN (
          SELECT mdod.dataflow_id, sum(mse.elapsed_ns) AS elapsed_ns
          FROM mz_dataflow_operator_dataflows AS mdod
          JOIN mz_scheduling_elapsed AS mse ON mse.id = mdod.id
          GROUP BY mdod.dataflow_id
        ) AS el ON el.dataflow_id = md.id
        ORDER BY size DESC`
        .$castTo<DataflowListRow>()
        .compile(queryBuilder),
    };
  }, [enabled]);

  const { results, error, databaseError, loading, refetch } = useSqlManyTyped(
    queries,
    {
      cluster: params?.clusterName,
      replica: params?.replicaName,
    },
  );

  // JSON tuple, not a delimiter-joined string: identifiers can themselves
  // contain any delimiter picked.
  const key = params
    ? JSON.stringify([params.clusterName, params.replicaName])
    : null;

  const compute = React.useCallback(
    (r: NonNullable<typeof results>) => normalize(r.list ?? []),
    [],
  );
  const data = useLastGoodByKey({ key, results, error, loading, compute });

  return { data, error, databaseError, loading, refetch };
}
