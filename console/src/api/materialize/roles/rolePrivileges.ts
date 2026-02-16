// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { sql } from "kysely";

import { queryBuilder } from "~/api/materialize/db";
import { executeSqlV2 } from "~/api/materialize/executeSqlV2";

export type RolePrivilegesParameters = {
  roleName: string;
};

export function buildRolePrivilegesQuery(params: RolePrivilegesParameters) {
  return sql<{
    grantor: string;
    grantee: string;
    database: string;
    schema: string;
    name: string;
    object_type: string;
    privilege_type: string;
  }>`SHOW PRIVILEGES FOR ${sql.id(params.roleName)}`.compile(queryBuilder);
}

export function fetchRolePrivileges({
  parameters,
  queryKey,
  requestOptions,
}: {
  parameters: RolePrivilegesParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildRolePrivilegesQuery(parameters);
  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });
}
