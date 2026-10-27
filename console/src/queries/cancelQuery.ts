// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";
import { useMutation } from "@tanstack/react-query";

import { buildRegionQueryKey } from "~/api/buildQueryKeySchema";
import { cancelQuery, CancelQueryParams } from "~/api/materialize/cancelQuery";

export const cancelQueryQueryKeys = {
  all: () => buildRegionQueryKey("cancelQuery"),
  connectionIdFromSessionId: (params: CancelQueryParams) =>
    [
      ...cancelQueryQueryKeys.all(),
      "connectionIdFromSessionId",
      params,
    ] as const,
  cancelQuery: (params: CancelQueryParams) =>
    [...cancelQueryQueryKeys.all(), "cancelQuery", params] as const,
};

export function useCancelQuery() {
  return useMutation({
    mutationKey: cancelQueryQueryKeys.all(),
    mutationFn: async (params: CancelQueryParams) => {
      return cancelQuery({
        params,
        getConnectionIdFromSessionIdQueryKey:
          cancelQueryQueryKeys.connectionIdFromSessionId(params),
        cancelQueryQueryKey: cancelQueryQueryKeys.cancelQuery(params),
      });
    },
    onError: (error, variables) => {
      Sentry.captureException(error, {
        extra: { variables },
      });
    },
  });
}
