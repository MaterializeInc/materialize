// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { RawBuilder } from "kysely";

import {
  CATALOG_SERVER_CLUSTER,
  executeSql,
  isExecuteSqlError,
  queryBuilder,
} from "~/api/materialize";

async function createConnection({
  connectionName,
  databaseName,
  schemaName,
  createConnectionQuery,
  environmentdHttpAddress,
}: {
  createConnectionQuery: RawBuilder<unknown>;
  connectionName: string;
  databaseName: string;
  schemaName: string;
  environmentdHttpAddress: string;
}) {
  const selectNewConnectionQuery = queryBuilder
    .selectFrom("mz_object_fully_qualified_names as o")
    .where("o.database_name", "=", databaseName)
    .where("o.schema_name", "=", schemaName)
    .where("o.name", "=", connectionName)
    .select("o.id")
    .compile();
  const createConnectionQueryCompiled =
    createConnectionQuery.compile(queryBuilder);
  const createConnectionResponse = await executeSql(environmentdHttpAddress, {
    queries: [
      {
        query: createConnectionQueryCompiled.sql,
        params: createConnectionQueryCompiled.parameters as string[],
      },
      {
        query: selectNewConnectionQuery.sql,
        params: selectNewConnectionQuery.parameters as string[],
      },
    ],
    cluster: CATALOG_SERVER_CLUSTER,
  });

  let response;

  if (isExecuteSqlError(createConnectionResponse)) {
    response = {
      error: createConnectionResponse,
    };
  } else {
    const [_, selectConnectionResponse] = createConnectionResponse.results;
    const [connectionId] = selectConnectionResponse.rows[0];
    response = {
      data: {
        connectionId: connectionId as string,
      },
    };
  }

  return response;
}

export default createConnection;
