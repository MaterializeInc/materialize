// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { getMaterializeClient } from "./materializeSqlClient";

type ObjectSearchParameters = {
  name: string;
  schemaName: string;
  databaseName: string;
};

type ClusterSearchParameters = {
  name: string;
};

// This function is used to get the id of an object or cluster by its name.
export async function getIdByName(
  params: ClusterSearchParameters | ObjectSearchParameters,
) {
  const client = await getMaterializeClient();

  if ("schemaName" in params) {
    const {
      rows: [object],
    } = await client.query(
      `
      select id
      from mz_internal.mz_object_fully_qualified_names
      where name = '${params.name}' AND
      schema_name = '${params.schemaName}' AND
      database_name = '${params.databaseName}'`,
    );

    return object.id as string;
  }
  const {
    rows: [cluster],
  } = await client.query(
    `select id from mz_clusters where name = '${params.name}'`,
  );
  return cluster.id as string;
}
