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

import { queryBuilder, useSqlTyped } from "~/api/materialize";

import { hasSuperUserPrivileges } from "./expressionBuilders";

export const USE_CAN_CREATE_OBJECTS_QUERY_KEY = "useCanCreateObjects";

/**
 * Checks system privileges to see if a user has create privileges in any schema
 */
export default function useCanCreateObjects() {
  const query = React.useMemo(() => {
    return sql<{
      canCreateObjects: boolean;
    }>`SELECT (${hasSuperUserPrivileges()}) OR EXISTS (
  SELECT * FROM mz_show_my_schema_privileges WHERE privilege_type = 'CREATE'
) as "canCreateObjects"`.compile(queryBuilder);
  }, []);
  const response = useSqlTyped(query, {
    queryKey: USE_CAN_CREATE_OBJECTS_QUERY_KEY,
  });

  let canCreateObjects = false;

  if (response.results && response.results[0]) {
    canCreateObjects = response.results[0].canCreateObjects;
  }

  return { ...response, results: canCreateObjects };
}
