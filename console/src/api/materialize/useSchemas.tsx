// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { parseSearchPath, useSqlTyped } from "~/api/materialize";

import { DEFAULT_DATABASE_NAME } from "./databaseList";
import { buildSchemaListQuery, Schema } from "./schemaList";

export const DEFAULT_SCHEMA_NAME = "public";

/**
 * Fetches all schemas, optionally filtered by database
 */
function useSchemas(
  options: { databaseId?: string; filterByCreatePrivilege?: boolean } = {},
) {
  const query = React.useMemo(() => {
    const qb = buildSchemaListQuery({
      databaseId: options.databaseId,
      filterByCreatePrivilege: options.filterByCreatePrivilege,
    });
    return qb.compile();
  }, [options.databaseId, options.filterByCreatePrivilege]);

  return useSqlTyped(query);
}

export function isDefaultSchema(schema: Schema) {
  return (
    schema.name === DEFAULT_SCHEMA_NAME &&
    schema.databaseName === DEFAULT_DATABASE_NAME
  );
}

/** Extracts and unescapes schema names from the a given search_path. */
export function getSchemaNameFromSearchPath(
  searchPath?: string,
  extantSchemaNames?: {
    name: Schema["name"];
  }[],
) {
  if (!searchPath || !extantSchemaNames) {
    return undefined;
  }
  const searchParts = parseSearchPath(searchPath);

  for (const potentialSchemaName of searchParts) {
    if (extantSchemaNames.some(({ name }) => name === potentialSchemaName)) {
      return potentialSchemaName;
    }
  }

  return undefined;
}

export type UseSchemaResponse = ReturnType<typeof useSchemas>;

export const useSchemasColumns = ["id", "name", "databaseId", "databaseName"];

export default useSchemas;
