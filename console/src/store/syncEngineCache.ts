// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { atom } from "jotai";
import { loadable } from "jotai/utils";

import {
  fetchCurrentOrganization,
  isOrganizationFetchEnabled,
  queryKeys as authQueryKeys,
} from "~/api/auth";
import { Organization } from "~/api/cloudGlobalApi";
import { appConfigAtom } from "~/config/store";
import { getQueryClient } from "~/queryClient";
import { currentRegionIdSyncAtom } from "~/store/environments";

/**
 * The auth/region scope for the sync-engine localStorage caches, as
 * `organizationId|regionId`. Resolves to undefined until both are known, so
 * consumers defer cache seeding until the scope is settled and never share one
 * tenant's cache with another on a shared origin.
 */
export const syncEngineCacheScopeAtom = atom(async (get) => {
  const regionId = get(currentRegionIdSyncAtom);
  if (!regionId) return undefined;

  const organizationId = isOrganizationFetchEnabled(get(appConfigAtom))
    ? (
        await getQueryClient().ensureQueryData<Organization>({
          queryKey: authQueryKeys.currentOrganization(),
          queryFn: fetchCurrentOrganization,
        })
      ).id
    : undefined;
  return `${organizationId ?? ""}|${regionId}`;
});

/** Synchronous view of the scope for store.sub consumers (subscribe sessions). */
export const syncEngineCacheScopeLoadableAtom = loadable(
  syncEngineCacheScopeAtom,
);
