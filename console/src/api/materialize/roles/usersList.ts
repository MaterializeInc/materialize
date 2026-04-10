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

/**
 * Builds a query to get all users (roles where rolcanlogin = true) with their assigned roles.
 * Users are roles that can log in (rolcanlogin = true).
 * System roles (mz_*) are excluded from results.
 */
export function buildUsersListQuery() {
  return queryBuilder
    .selectFrom("mz_roles as u")
    .leftJoinLateral(
      (eb) =>
        eb
          .selectFrom("mz_role_members as rm")
          .innerJoin(
            "mz_roles as assigned_role",
            "assigned_role.id",
            "rm.role_id",
          )
          .select(
            sql<
              string[] | null
            >`list_agg(assigned_role.name ORDER BY assigned_role.name)`.as(
              "roles_granted",
            ),
          )
          .whereRef("rm.member", "=", "u.id")
          .where("assigned_role.id", "like", "u%")
          .as("roles"),
      (join) => join.onTrue(),
    )
    .select(["u.id", "u.name as user_name", "roles.roles_granted"])
    .where("u.name", "not like", "mz_%")
    .where(sql<boolean>`COALESCE(u.rolcanlogin, false) = true`)
    .orderBy("user_name");
}

export async function fetchUsersList({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildUsersListQuery().compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });
}

export type UserItem = InferResult<ReturnType<typeof buildUsersListQuery>>[0];
