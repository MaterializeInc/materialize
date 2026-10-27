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
  buildFullyQualifiedObjectName,
  queryBuilder,
  SchemaObject,
  useSqlTyped,
} from "~/api/materialize";

export type DDLNoun = "SINK" | "SOURCE";

/**
 * Fetches the DDL statement for creating a schema object
 */
function useShowCreate(noun: DDLNoun, schemaObject?: SchemaObject) {
  const query = React.useMemo(() => {
    if (!schemaObject) return null;

    return sql<{
      name: string;
      create_sql: string;
    }>`SHOW CREATE ${sql.raw(noun)} ${buildFullyQualifiedObjectName(
      schemaObject,
    )}`.compile(queryBuilder);
  }, [noun, schemaObject]);

  const response = useSqlTyped(query);

  let ddl: string | null = null;
  if (response.results) {
    ddl = response.results[0].create_sql;
  }

  return { ...response, results: ddl };
}

export default useShowCreate;
