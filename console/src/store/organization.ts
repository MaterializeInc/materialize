// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { atom } from "jotai";

import {
  fetchCurrentOrganization,
  queryKeys as authQueryKeys,
} from "~/api/auth";
import { Organization } from "~/api/cloudGlobalApi";
import { appConfigAtom } from "~/config/store";
import { getQueryClient } from "~/queryClient";

import { currentEnvironmentState } from "./environments";

export const isCurrentOrganizationBlockedAtom = atom(async (get) => {
  const appConfig = get(appConfigAtom);
  if (appConfig.mode === "self-managed" || appConfig.isImpersonating)
    // The current organization should never be blocked in self-managed or impersonation mode.
    return false;

  const currentOrganization =
    await getQueryClient().ensureQueryData<Organization>({
      queryKey: authQueryKeys.currentOrganization(),
      queryFn: fetchCurrentOrganization,
    });
  if (currentOrganization.blocked) return true;
  const currentEnvironment = await get(currentEnvironmentState);
  return (
    currentEnvironment?.state === "enabled" &&
    currentEnvironment.status.health === "blocked"
  );
});
