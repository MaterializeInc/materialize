// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { atom, useAtomValue } from "jotai";
import React, { useMemo } from "react";

import { buildSubscribeQuery } from "~/api/materialize/buildSubscribeQuery";
import {
  buildRolesListQuery,
  RoleItem,
} from "~/api/materialize/roles/rolesList";
import {
  SubscribeRow,
  SubscribeState,
} from "~/api/materialize/SubscribeManager";
import { useGlobalUpsertSubscribe } from "~/api/materialize/useSubscribe";

export { type RoleItem };

export const allRoles = atom<SubscribeState<RoleItem>>({
  data: [],
  error: undefined,
  snapshotComplete: false,
});

export function useSubscribeToAllRoles() {
  const subscribe = useMemo(() => {
    return buildSubscribeQuery(buildRolesListQuery(), {
      upsertKey: ["roleName"],
    });
  }, []);

  return useGlobalUpsertSubscribe({
    atom: allRoles,
    subscribe,
    select: (row: SubscribeRow<RoleItem>) => row.data,
    upsertKey: (row: SubscribeRow<RoleItem>) => row.data.roleName,
  });
}

export function useAllRoles() {
  const result = useAtomValue(allRoles);
  return React.useMemo(() => {
    return {
      ...result,
      data: result.data,
      isError: Boolean(result.error),
      snapshotComplete: result.snapshotComplete,
      isLoading: !result.snapshotComplete && !result.error,
    };
  }, [result]);
}
