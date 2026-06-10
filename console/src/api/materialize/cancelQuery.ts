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

import { executeSqlV2 } from "~/api/materialize";

import { queryBuilder } from "./db";
import { assertAtLeastOneRow } from "./NoRowsError";

export function buildGetConnectionIdFromSessionIdQuery(sessionId: string) {
  return queryBuilder
    .selectFrom("mz_sessions")
    .select("connection_id")
    .distinct()
    .where("id", "=", sessionId);
}

export function buildCancelQuery(connectionId: string) {
  return sql<{
    pg_cancel_backend: boolean;
  }>`SELECT pg_cancel_backend(${sql.raw(connectionId)})`;
}

export type CancelQueryParams =
  | { connectionId: string }
  | { sessionId: string };

export async function cancelQuery({
  params,
  getConnectionIdFromSessionIdQueryKey,
  cancelQueryQueryKey,
}: {
  params: CancelQueryParams;
  getConnectionIdFromSessionIdQueryKey: QueryKey;
  cancelQueryQueryKey: QueryKey;
}) {
  let connectionId;
  if ("sessionId" in params) {
    const { sessionId } = params;

    const { rows } = await executeSqlV2({
      queries: buildGetConnectionIdFromSessionIdQuery(sessionId).compile(),
      queryKey: getConnectionIdFromSessionIdQueryKey,
    });

    assertAtLeastOneRow(rows.length);

    connectionId = rows[0].connection_id;
  } else {
    connectionId = params.connectionId;
  }

  const { rows } = await executeSqlV2({
    queries: buildCancelQuery(connectionId).compile(queryBuilder),
    queryKey: cancelQueryQueryKey,
  });
  assertAtLeastOneRow(rows.length);

  if (!rows[0].pg_cancel_backend) {
    throw new Error("Cancel not successful");
  }
}
