// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";

import { executeSqlV2 } from "~/api/materialize/executeSqlV2";

import { buildRoleMembershipQuery, RoleMembershipItem } from "./roleMembers";

export interface RoleGraphEdge {
  parentName: string;
  childName: string;
}

export function buildRoleGraphEdgesQuery() {
  return buildRoleMembershipQuery({
    excludeSystemRoles: true,
    memberType: "roles",
  });
}

export async function fetchRoleGraphEdges({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildRoleGraphEdgesQuery().compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });
}

export type RoleGraphEdgeItem = RoleMembershipItem;
