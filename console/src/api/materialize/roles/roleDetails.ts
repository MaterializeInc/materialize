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

export type RoleDetailsParameters = {
  roleName: string;
};

export function buildRoleDetailsQuery(params: RoleDetailsParameters) {
  return queryBuilder
    .selectFrom("mz_roles as r")
    .leftJoinLateral(
      (eb) =>
        eb
          .selectFrom("mz_role_members as m")
          .innerJoin("mz_roles as member_role", "member_role.id", "m.member")
          .select(
            sql<
              string[] | null
            >`list_agg(member_role.name ORDER BY member_role.name) FILTER (WHERE member_role.rolcanlogin)`.as(
              "users",
            ),
          )
          .whereRef("m.role_id", "=", "r.id")
          .as("uc"),
      (join) => join.onTrue(),
    )
    .leftJoinLateral(
      (eb) =>
        eb
          .selectFrom("mz_role_members as m")
          .innerJoin("mz_roles as member_role", "member_role.id", "m.member")
          .select(
            sql<
              string[] | null
            >`list_agg(member_role.name ORDER BY member_role.name) FILTER (WHERE NOT member_role.rolcanlogin)`.as(
              "grantedToRoles",
            ),
          )
          .whereRef("m.role_id", "=", "r.id")
          .as("gc"),
      (join) => join.onTrue(),
    )
    .leftJoinLateral(
      (eb) =>
        eb
          .selectFrom("mz_role_members as rm")
          .innerJoin("mz_roles as parent", "parent.id", "rm.role_id")
          .select(
            sql<string[] | null>`list_agg(parent.name ORDER BY parent.name)`.as(
              "grantedRoles",
            ),
          )
          .whereRef("rm.member", "=", "r.id")
          .as("gr"),
      (join) => join.onTrue(),
    )
    .select(["r.name", "uc.users", "gc.grantedToRoles", "gr.grantedRoles"])
    .where("r.name", "=", params.roleName);
}

export function fetchRoleDetails({
  parameters,
  queryKey,
  requestOptions,
}: {
  parameters: RoleDetailsParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildRoleDetailsQuery(parameters).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });
}

export type RoleDetails = InferResult<
  ReturnType<typeof buildRoleDetailsQuery>
>[0];
