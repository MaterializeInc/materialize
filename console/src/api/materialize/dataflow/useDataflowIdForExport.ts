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

import { queryBuilder } from "../db";

export interface DataflowIdForExportParams {
  clusterName: string;
  replicaName: string;
  exportId: string;
}

interface DataflowIdRow {
  dataflowId: string;
}

export function useDataflowIdForExport(params?: DataflowIdForExportParams) {
  const exportId = params?.exportId;
  const queries = React.useMemo(() => {
    // Export ids are of the form s<n>/t<n>/u<n>. Reject anything else before
    // interpolating so a malformed param can never reach the query.
    if (exportId === undefined || !/^[stu]\d+$/.test(exportId)) return null;
    return {
      row: sql`
        SELECT dataflow_id::text AS "dataflowId"
        FROM mz_compute_exports
        WHERE export_id = ${lit(exportId)}`
        .$castTo<DataflowIdRow>()
        .compile(queryBuilder),
    };
  }, [exportId]);

  const { results, error, databaseError, loading } = useSqlManyTyped(queries, {
    cluster: params?.clusterName,
    replica: params?.replicaName,
  });

  const dataflowId =
    !error && results ? (results.row?.[0]?.dataflowId ?? null) : null;
  return { dataflowId, loading, error, databaseError };
}
