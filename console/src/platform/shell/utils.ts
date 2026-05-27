// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { createNamespace } from "~/api/materialize";
import { SchemaWithOptionalDatabase as Schema } from "~/api/materialize/schemaList";
import { getSchemaNameFromSearchPath } from "~/api/materialize/useSchemas";

export function getSelectedSchemaOption(
  searchPath?: string,
  database?: string | null,
  schemas?: Schema[],
) {
  const schemaName = getSchemaNameFromSearchPath(searchPath, schemas);

  if (!schemaName || !schemas || !database) {
    return null;
  }

  const doesSchemaExist = schemas.some(
    ({ databaseName, name }) =>
      databaseName === database && name === schemaName,
  );

  if (!doesSchemaExist) {
    return null;
  }

  return {
    id: createNamespace(database, schemaName),
    name: schemaName,
    databaseName: database ?? "",
  };
}
