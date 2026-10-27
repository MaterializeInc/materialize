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

export type RoleMembershipFilters = {
  parentRoleName?: string;
  childRoleName?: string;
  excludeSystemRoles?: boolean;
  /** Filter members by type: "users" (can login) or "roles" (cannot login) */
  memberType?: "users" | "roles";
};

export function buildRoleMembershipQuery(filters: RoleMembershipFilters = {}) {
  let qb = queryBuilder
    .selectFrom("mz_role_members as rm")
    .innerJoin("mz_roles as parent", "parent.id", "rm.role_id")
    .innerJoin("mz_roles as child", "child.id", "rm.member")
    .select([
      "child.id as id",
      "parent.name as roleName",
      "child.name as memberName",
      "child.name as childRoleName",
      // Keep parentName/childName for DagreGraphEdge compatibility
      "parent.name as parentName",
      "child.name as childName",
    ])
    .orderBy("child.name");

  if (filters.parentRoleName) {
    qb = qb.where("parent.name", "=", filters.parentRoleName);
  }

  if (filters.childRoleName) {
    qb = qb.where("child.name", "=", filters.childRoleName);
  }

  if (filters.excludeSystemRoles) {
    qb = qb.where("parent.id", "like", "u%").where("child.id", "like", "u%");
  }

  if (filters.memberType === "users") {
    qb = qb.where(sql<boolean>`child.rolcanlogin`);
  }

  if (filters.memberType === "roles") {
    qb = qb.where(sql<boolean>`NOT child.rolcanlogin`);
  }

  return qb;
}

export function fetchRoleMembership({
  filters,
  queryKey,
  requestOptions,
}: {
  filters: RoleMembershipFilters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildRoleMembershipQuery(filters).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });
}

export type RoleMembershipItem = InferResult<
  ReturnType<typeof buildRoleMembershipQuery>
>[0];

export type RoleMembersParameters = {
  roleName: string;
  memberType?: "users" | "roles";
};

export function buildRoleMembersQuery(params: RoleMembersParameters) {
  return buildRoleMembershipQuery({
    parentRoleName: params.roleName,
    memberType: params.memberType,
  });
}

export function fetchRoleMembers({
  parameters,
  queryKey,
  requestOptions,
}: {
  parameters: RoleMembersParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  return fetchRoleMembership({
    filters: {
      parentRoleName: parameters.roleName,
      memberType: parameters.memberType,
    },
    queryKey,
    requestOptions,
  });
}

export type RoleMember = RoleMembershipItem;
