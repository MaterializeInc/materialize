// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { InferResult } from "kysely";

import { executeSqlV2 } from "~/api/materialize/executeSqlV2";
import { getOwners } from "~/api/materialize/expressionBuilders";

/**
 * Fetches all roles along with whether the current user can act as each role.
 *
 * Ownership can't be computed inside a SUBSCRIBE because has_role() and
 * mz_is_superuser() can't run in a dataflow, so subscribe consumers join
 * this against a row's owner id client-side.
 */
export async function fetchOwners({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = getOwners().compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });
}

export type Owner = InferResult<ReturnType<typeof getOwners>>[0];
