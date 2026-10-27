// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useSuspenseQuery } from "@tanstack/react-query";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import fetchMaxMySqlConnections from "~/api/materialize/connection/maxMySqlConnections";
import { ConnectionsQueryFilter } from "~/api/materialize/connection/useConnections";

export const connectionQueryKeys = {
  all: () => buildRegionQueryKey("connection"),
  list: (filters: ConnectionsQueryFilter) =>
    [...connectionQueryKeys.all(), buildQueryKeyPart("list", filters)] as const,
  maxMySqlConnections: () =>
    [
      ...connectionQueryKeys.all(),
      buildQueryKeyPart("maxMySqlConnections"),
    ] as const,
};

export function useMaxMySqlConnections() {
  return useSuspenseQuery({
    queryKey: connectionQueryKeys.maxMySqlConnections(),
    queryFn: ({ queryKey, signal }) => {
      return fetchMaxMySqlConnections({ queryKey, requestOptions: { signal } });
    },
  });
}
