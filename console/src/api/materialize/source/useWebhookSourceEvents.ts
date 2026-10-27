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

import {
  buildSubscribeQuery,
  useSubscribeManager,
} from "~/api/materialize/useSubscribe";

function useWebhookSourceEvents({
  databaseName,
  schemaName,
  sourceName,
  clusterName,
  headerColumns,
}: {
  databaseName: string;
  schemaName: string;
  sourceName: string;
  clusterName: string;
  headerColumns: string[];
}) {
  const subscribe = React.useMemo(() => {
    const selectCols = [sql.id("body"), ...headerColumns.map((c) => sql.id(c))];
    const selectClause = sql.join(selectCols, sql`, `);
    return buildSubscribeQuery(
      sql<{
        body: object;
        headers: object;
      }>`SELECT ${selectClause} FROM ${sql.id(databaseName)}.${sql.id(schemaName)}.${sql.id(sourceName)}`,
      { upsertKey: "body" },
    );
  }, [databaseName, headerColumns, schemaName, sourceName]);
  const results = useSubscribeManager({ subscribe, clusterName });
  return results;
}

export default useWebhookSourceEvents;
