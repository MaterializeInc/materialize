// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { InferResult } from "kysely";

import { queryBuilder } from "..";

function getSourceByNameStatement(
  name: string,
  databaseName: string,
  schemaName: string,
) {
  return queryBuilder
    .selectFrom("mz_object_fully_qualified_names as o")
    .where("object_type", "=", "source")
    .where("o.database_name", "=", databaseName)
    .where("o.schema_name", "=", schemaName)
    .where("o.name", "=", name)
    .select([
      "o.id",
      "o.database_name as databaseName",
      "o.schema_name as schemaName",
      "o.name",
    ]);
}

export const getSourceByNameColumns = [
  "id",
  "databaseName",
  "schemaName",
  "name",
];

export type SourceByNameResult = InferResult<
  ReturnType<typeof getSourceByNameStatement>
>[0];

export default getSourceByNameStatement;
