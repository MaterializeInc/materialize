// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CompiledQuery } from "kysely";
import { Client, ClientConfig } from "pg";
import { inject } from "vitest";

import { executeSqlV2, SEARCH_PATH } from "~/api/materialize";
import { ExecuteSqlRequest } from "~/api/materialize/executeSqlV2";

export const QUICKSTART_CLUSTER = "quickstart";

/**
 * Returns a connected node-pg client. Useful for running one off queries. Queries under
 * test should use `executeSqlHttp` if possible, since that exercises more of our real
 * code paths.
 */
export async function getMaterializeClient(options?: ClientConfig) {
  const {
    internalSql: { host, port },
  } = inject("materializeEndpoints");
  const client = new Client({
    user: "mz_system",
    host,
    port,
    database: "materialize",
    ...options,
  });
  await client.connect();
  await client.query(`SET search_path TO ${SEARCH_PATH}`);
  return client;
}

/**
 * Executes a compiled kysely query over http
 */
export async function executeSqlHttp<Q extends CompiledQuery>(
  query: Q,
  options: Omit<ExecuteSqlRequest<Q>, "queries" | "queryKey"> = {},
) {
  const {
    internalHttp: { host, port },
  } = inject("materializeEndpoints");
  return executeSqlV2({
    queries: query,
    queryKey: [], // dummy query key, since we aren't using RQ
    httpAddress: `${host}:${port}`,
    ...options,
  });
}
