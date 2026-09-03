// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useAtomValue } from "jotai";

import { useMaybeCurrentOrganizationId } from "~/api/auth";
import { currentRegionIdSyncAtom } from "~/store/environments";

/**
 * The auth/region scope for the sync-engine localStorage caches, as
 * `organizationId|regionId`. Returns undefined until both are known, so callers
 * defer seeding until the scope is settled and never share one tenant's cache
 * with another on the shared `console.materialize.com` origin.
 */
export function useSyncEngineCacheScope(): string | undefined {
  const maybeOrganizationId = useMaybeCurrentOrganizationId();
  const regionId = useAtomValue(currentRegionIdSyncAtom);

  const organizationIdLoading =
    maybeOrganizationId !== null && maybeOrganizationId.isLoading;
  if (organizationIdLoading || !regionId) return undefined;

  const organizationId =
    maybeOrganizationId !== null ? maybeOrganizationId.data : undefined;
  return `${organizationId ?? ""}|${regionId}`;
}
