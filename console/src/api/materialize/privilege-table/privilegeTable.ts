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
import { hasSuperUserPrivileges } from "~/api/materialize/expressionBuilders";
import { STATEMENT_LIFECYCLE_TABLE } from "~/api/materialize/query-history/queryHistoryDetail";
import {
  QUERY_HISTORY_LIST_TABLE,
  QUERY_HISTORY_LIST_TABLE_REDACTED,
} from "~/api/materialize/query-history/queryHistoryList";

export function buildIsSuperuserQuery() {
  return queryBuilder.selectNoFrom(
    sql<boolean>`(${hasSuperUserPrivileges()})`.as("isSuperUser"),
  );
}

export type Privilege = "SELECT" | "INSERT" | "UPDATE" | "DELETE";

/**
 * OIDs for relations used in privilege checks, to avoid name-based lookups.
 * (In the Materialize repo, these are constants like VIEW_MZ_RECENT_ACTIVITY_LOG_OID.)
 * Name-based lookups are slower, because then a string-to-regclass cast happens, see
 * https://github.com/MaterializeInc/materialize/blob/8a6dc1c498b131f7255c574fa2ac3823b2d8577c/src/sql/src/func.rs#L2163
 * which is a complicated SQL query, see
 * https://github.com/MaterializeInc/materialize/blob/8a6dc1c498b131f7255c574fa2ac3823b2d8577c/src/sql/src/plan/typeconv.rs#L136-L175
 */
export const RELATION_OIDS = {
  [QUERY_HISTORY_LIST_TABLE]: 16748,
  [STATEMENT_LIFECYCLE_TABLE]: 16750,
} as const;

/**
 *
 * Queries a privilege table of the following structure:
 *
 * relation             | privilege | hasPrivilege
 * ------------------------------------------------
 * mz_secrets           | select    | true
 *
 * Each relation must exist in mz_relations.
 *
 * In the future, we can expand this function to objects other than relations.
 */
export function buildPrivilegeTableQuery() {
  return sql<{
    relation: string;
    privilege: string;
    hasTablePrivilege: boolean;
  }>`
SELECT DISTINCT relation, privilege, "hasTablePrivilege"
FROM
    (
        VALUES
            ('${sql.raw(QUERY_HISTORY_LIST_TABLE)}', 'SELECT', has_table_privilege(${sql.raw(String(RELATION_OIDS[QUERY_HISTORY_LIST_TABLE]))}, 'SELECT')),
            ('${sql.raw(QUERY_HISTORY_LIST_TABLE_REDACTED)}', 'SELECT', true),
            ('${sql.raw(STATEMENT_LIFECYCLE_TABLE)}', 'SELECT', has_table_privilege(${sql.raw(String(RELATION_OIDS[STATEMENT_LIFECYCLE_TABLE]))}, 'SELECT'))
    )
    AS _ (relation, privilege, "hasTablePrivilege");
  `;
}

/**
 * Fetches the privilege table and super user status.
 */
export async function fetchPrivilegeTable({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const isSuperUserQuery = buildIsSuperuserQuery().compile();
  const privilegeTableQuery = buildPrivilegeTableQuery().compile(queryBuilder);

  return executeSqlV2({
    queries: [isSuperUserQuery, privilegeTableQuery] as const,
    queryKey: queryKey,
    requestOptions,
  });
}
export type PrivilegeTable = {
  relation: string;
  privilege: string;
  hasTablePrivilege: boolean;
}[];

export type PrivilegeRow = PrivilegeTable[0];
