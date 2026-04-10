// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { InferResult, sql } from "kysely";

import { queryBuilder } from "~/api/materialize/db";
import { executeSqlV2 } from "~/api/materialize/executeSqlV2";

export function buildRolesListQuery() {
  return queryBuilder
    .selectFrom("mz_roles as r")
    .leftJoinLateral(
      (eb) =>
        eb
          .selectFrom("mz_role_members as rm")
          .innerJoin("mz_roles as member_role", "member_role.id", "rm.member")
          .select((sub) => sub.fn.count("rm.member").as("member_count"))
          .whereRef("rm.role_id", "=", "r.id")
          .where(sql<boolean>`member_role.rolcanlogin`)
          .as("counts"),
      (join) => join.onTrue(),
    )
    .leftJoinLateral(
      (eb) =>
        eb
          .selectFrom("mz_objects as o")
          .select((sub) => sub.fn.count("o.id").as("owned_count"))
          .whereRef("o.owner_id", "=", "r.id")
          .as("owned"),
      (join) => join.onTrue(),
    )
    .select([
      "r.id as roleId",
      "r.name as roleName",
      sql<number>`COALESCE(counts.member_count, 0)`.as("memberCount"),
      sql<number>`COALESCE(owned.owned_count, 0)`.as("ownedObjectsCount"),
    ])
    .where("r.name", "not like", "mz_%")
    .where(sql<boolean>`NOT r.rolcanlogin`)
    .orderBy("roleName");
}

export async function fetchRolesList({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildRolesListQuery().compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });
}

export type RoleItem = InferResult<ReturnType<typeof buildRolesListQuery>>[0];
