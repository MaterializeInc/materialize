// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey, useQuery } from "@tanstack/react-query";
import { sql } from "kysely";

import { buildRegionQueryKey } from "~/api/buildQueryKeySchema";

import { queryBuilder } from "./db";
import { executeSqlV2 } from "./executeSqlV2";

export const currentUserQueryKeys = {
  all: () => buildRegionQueryKey("currentUser"),
};

function buildCurrentUserQuery() {
  return queryBuilder.selectNoFrom(sql<string>`current_user`.as("currentUser"));
}

async function fetchCurrentUser({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  return executeSqlV2({
    queries: buildCurrentUserQuery().compile(),
    queryKey,
    requestOptions,
  });
}

/** Fetches the SQL role the connected session is acting as. */
export default function useCurrentUser() {
  const { data, isLoading, error } = useQuery({
    queryKey: currentUserQueryKeys.all(),
    queryFn: ({ queryKey, signal }) =>
      fetchCurrentUser({ queryKey, requestOptions: { signal } }),
    staleTime: Infinity,
  });

  return {
    results: data?.rows[0]?.currentUser,
    isLoading,
    error,
  };
}
