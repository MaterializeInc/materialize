// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useQuery } from "@tanstack/react-query";
import { useCallback } from "react";

import { buildRegionQueryKey } from "~/api/buildQueryKeySchema";
import {
  fetchPrivilegeTable,
  Privilege,
} from "~/api/materialize/privilege-table/privilegeTable";

export const privilegesQueryKeys = {
  privileges: () => [...buildRegionQueryKey("privileges")] as const,
};

export type HasPrivilegeCallback = (privilegeInfo: {
  relation: string;
  privilege: Privilege;
}) => boolean;

/**
 * Hook used to get a user's privileges in Materialize.
 *
 * More information about privileges can be found here: https://materialize.com/docs/manage/access-control/rbac/
 */
export function usePrivileges() {
  const {
    data: privilegeData,
    isLoading,
    isSuccess,
    isError,
    error,
  } = useQuery({
    queryKey: privilegesQueryKeys.privileges(),
    queryFn: async ({ queryKey, signal }) => {
      const data = await fetchPrivilegeTable({
        queryKey,
        requestOptions: { signal },
      });
      const [isSuperUserRes, privilegesRes] = data;

      return {
        isSuperUser: isSuperUserRes.rows[0].isSuperUser,
        privilegeTable: privilegesRes.rows,
      };
    },
    // Disable refetching
    staleTime: Number.POSITIVE_INFINITY,
    gcTime: Number.POSITIVE_INFINITY,
  });

  const hasPrivilege = useCallback<HasPrivilegeCallback>(
    ({ relation, privilege }) => {
      if (!privilegeData) {
        return false;
      }

      if (privilegeData.isSuperUser) {
        return true;
      }

      return privilegeData.privilegeTable.some(
        (privilegeRow) =>
          privilegeRow.relation === relation &&
          privilegeRow.privilege === privilege &&
          privilegeRow.hasTablePrivilege,
      );
    },
    [privilegeData],
  );

  return {
    hasPrivilege,
    isSuperUser: privilegeData?.isSuperUser,
    isLoading,
    isSuccess,
    isError,
    error,
  };
}
