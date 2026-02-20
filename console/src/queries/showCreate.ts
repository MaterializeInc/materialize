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
import { assertExactlyOneRow } from "~/api/materialize/assertExactlyOneRow";
import {
  fetchShowCreate,
  ShowCreateStatementParameters,
} from "~/api/materialize/showCreate";

export const showCreateQueryKey = (params: ShowCreateStatementParameters) =>
  [
    ...buildRegionQueryKey("showCreate"),
    buildQueryKeyPart("showCreate", params),
  ] as const;

/**
 * Exectues a `SHOW CREATE` statement.
 */
export function useShowCreate(params: ShowCreateStatementParameters) {
  const { data, ...rest } = useSuspenseQuery({
    refetchInterval: 5000,
    queryKey: showCreateQueryKey(params),
    queryFn: async ({ queryKey, signal }) => {
      const [, parameters] = queryKey;
      const result = await fetchShowCreate(queryKey, parameters, { signal });

      assertExactlyOneRow(result.rows.length, { skipQueryRetry: true });
      return result;
    },
  });
  return { data: data.rows[0], ...rest };
}
