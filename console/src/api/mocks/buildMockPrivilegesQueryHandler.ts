// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";

import { PrivilegeRow } from "~/api/materialize/privilege-table/privilegeTable";
import { privilegesQueryKeys } from "~/hooks/usePrivileges";

import {
  buildColumn,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "./buildSqlQueryHandler";

export const fetchPrivilegeTableHandlerColumns = [
  [
    buildColumn({
      name: "isSuperUser",
    }),
  ],
  [
    buildColumn({
      name: "relation",
    }),
    buildColumn({
      name: "privilege",
    }),
    buildColumn({
      name: "hasTablePrivilege",
    }),
  ],
];

export function buildMockPrivilegesQueryHandler({
  privileges,
  isSuperUser,
  queryKey,
}: {
  isSuperUser?: boolean;
  privileges?: PrivilegeRow[];
  queryKey?: QueryKey;
}) {
  const fetchPrivilegeTableHandlerResults = [
    mapKyselyToTabular({
      columns: fetchPrivilegeTableHandlerColumns[0],
      rows: [{ isSuperUser: isSuperUser ?? true }],
    }),
    mapKyselyToTabular({
      columns: fetchPrivilegeTableHandlerColumns[1],
      rows: privileges ?? [],
    }),
  ];

  const fetchPrivilegeTableHandler = buildSqlQueryHandlerV2({
    queryKey: queryKey ?? privilegesQueryKeys.privileges(),
    results: fetchPrivilegeTableHandlerResults,
  });

  return fetchPrivilegeTableHandler;
}
