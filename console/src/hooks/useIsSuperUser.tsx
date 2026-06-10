// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useQuery } from "@tanstack/react-query";

import { buildRegionQueryKey } from "~/api/buildQueryKeySchema";
import { executeSqlV2 } from "~/api/materialize/executeSqlV2";
import { buildIsSuperuserQuery } from "~/api/materialize/privilege-table/privilegeTable";

export const isSuperUserQueryKeys = {
  isSuperUser: () => [...buildRegionQueryKey("isSuperUser")] as const,
};

/**
 * Checks if the current user has superuser privileges.
 * Use instead of usePrivileges() when you only need isSuperUser.
 */
export function useIsSuperUser() {
  const {
    data: isSuperUser,
    isLoading,
    isSuccess,
    isError,
    error,
  } = useQuery({
    queryKey: isSuperUserQueryKeys.isSuperUser(),
    queryFn: async ({ queryKey, signal }) => {
      const isSuperUserQuery = buildIsSuperuserQuery().compile();

      const [result] = await executeSqlV2({
        queries: [isSuperUserQuery] as const,
        queryKey: queryKey,
        requestOptions: { signal },
      });

      return result.rows[0].isSuperUser;
    },
    staleTime: Number.POSITIVE_INFINITY,
    gcTime: Number.POSITIVE_INFINITY,
  });

  return {
    isSuperUser,
    isLoading,
    isSuccess,
    isError,
    error,
  };
}
