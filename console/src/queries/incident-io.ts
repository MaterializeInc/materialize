// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useSuspenseQuery } from "@tanstack/react-query";

import { buildGlobalQueryKey } from "~/api/buildQueryKeySchema";
import { fetchSummary } from "~/api/incident-io";

export const incidentIOQueryKeys = {
  all: () => buildGlobalQueryKey("incident-io"),
};

export function useSummary() {
  return useSuspenseQuery({
    queryKey: incidentIOQueryKeys.all(),
    refetchIntervalInBackground: true,
    refetchInterval: 60_000,
    refetchOnReconnect: true,
    queryFn: ({ signal }) => fetchSummary({ signal }),
  });
}
