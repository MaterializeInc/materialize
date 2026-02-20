// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryClient } from "@tanstack/react-query";

import { appConfigAtom } from "./config/store";
import { getStore } from "./jotai";

export function buildQueryClient() {
  const { reactQueryRetriesEnabled } = getStore().get(appConfigAtom);

  return new QueryClient({
    defaultOptions: {
      queries: {
        retry: (failureCount, error) => {
          // Retries are disabled in test
          if (!reactQueryRetriesEnabled) return false;
          if (error.cause && (error.cause as any).status === 401) {
            // When a request is unauthorized, we want to fail fast and refresh the token.
            // In practice, the token should be refreshed by the middleware, so we should
            // only see this on the actual refresh token call which doesn't have the
            // middleware.
            return false;
          }
          // Some custom error have a `skipQueryRetry` field to skip retries,
          // e.g. NoRowsError.
          if (
            error instanceof Error &&
            "skipQueryRetry" in error &&
            error.skipQueryRetry
          ) {
            return false;
          }
          return failureCount < 3;
        },
      },
    },
  });
}

let queryClient = buildQueryClient();

export const resetQueryClient = () => {
  queryClient = buildQueryClient();
};

export const getQueryClient = () => queryClient;
