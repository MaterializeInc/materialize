// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";

import { buildFullyQualifiedObjectName, SchemaObject } from "~/api/materialize";
import { Cluster } from "~/api/materialize/cluster/useConnectorClusters";
import { assert } from "~/util";

export interface CreateSourceParameters {
  name: string;
  connection: SchemaObject;
  databaseName: string;
  schemaName: string;
  cluster: Cluster;
  allTables: boolean;
  tables: {
    schemaName: string;
    name: string;
    alias: string;
  }[];
}

const createSqlServerSourceStatement = (params: CreateSourceParameters) => {
  if (params.cluster.id === "0") {
    throw new Error("You must specify cluster");
  }

  assert(params.connection?.name);
  assert(params.cluster?.name);
  const query = sql`
CREATE SOURCE ${buildFullyQualifiedObjectName(params)}
IN CLUSTER ${sql.id(params.cluster.name)}
FROM SQL SERVER CONNECTION ${buildFullyQualifiedObjectName(params.connection)}
${
  params.allTables
    ? sql`FOR ALL TABLES`
    : sql`FOR TABLES (${sql.join(
        params.tables.map(
          (t) =>
            sql`${sql.id(t.schemaName)}.${sql.id(t.name)}${t.alias ? sql` AS ${sql.id(t.alias)}` : sql``}`,
        ),
      )})`
};`;

  return query;
};

export default createSqlServerSourceStatement;
