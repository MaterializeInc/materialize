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

export interface DropRoleVariables {
  roleName: string;
}

export function buildDropRoleStatement({ roleName }: DropRoleVariables) {
  return sql`DROP ROLE ${sql.id(roleName)}`;
}

export async function dropRole({
  variables,
  queryKey,
  requestOptions,
}: {
  variables: DropRoleVariables;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildDropRoleStatement(variables).compile(queryBuilder);
  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });
}
