// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { getStore } from "~/jotai";
import {
  currentRegionIdAtomLoadable,
  environmentsWithHealth,
} from "~/store/environments";

/**
 *
 * @param scope - Serves as a namespace for the query key part. Helps for logging, testing, and differentiating.
 * @param queryKeyPart - The original query key part. A query key part is an object in the query key array.
 * @returns - The original query key part with 'scope' attached to it
 */
export function buildQueryKeyPart<
  TQueryKeyPart extends Record<string, unknown> = Record<string, never>,
>(
  scope: string,
  queryKeyPart: TQueryKeyPart = {} as TQueryKeyPart,
): TQueryKeyPart & { scope: string } {
  return {
    scope,
    ...(queryKeyPart ?? {}),
  };
}

/**
 * A function that takes a scope (used for identification) and returns a query key with the region id.
 * This is useful for invalidating the cache by environment.
 *
 * Usage example:
 * ```
 *  export const secretQueryKeys = {
 *    all: () => buildRegionQueryKey({ scope: "secrets" }),
 *    create: () => [...secretQueryKeys.all(), buildQueryKeyPart("create")] as const,
 *  };
 *
 *  secretQueryKeys.all(); // [{scope: "secrets", regionId: "aws/us-east-1"}];
 *  secretQueryKeys.create(); // [{scope: "secrets", regionId: "aws/us-east-1"}, {scope: "create"}];
 * ```
 *
 * Note: It's important that this function is called after app initialization. This is because
 * running it on code download would throw an exception since Jotai state isn't loaded
 * at that point.
 *
 */
export function buildRegionQueryKey(scope: string, regionId?: string) {
  const store = getStore();
  const maybeRegionId = store.get(currentRegionIdAtomLoadable);
  const currentRegionId =
    maybeRegionId.state === "hasData" ? maybeRegionId.data : "no-region";

  // Get the environment version if available
  const environments = store.get(environmentsWithHealth);
  const environment = environments?.get(regionId ?? currentRegionId);
  const environmentVersion =
    environment &&
    environment.state === "enabled" &&
    environment.status.health === "healthy"
      ? environment.status.version.crateVersion.version
      : undefined;

  return [
    buildQueryKeyPart(scope, {
      regionId: regionId ?? currentRegionId,
      environmentVersion,
    }),
  ] as const;
}

/**
 * A function that takes a scope (used for identification) and returns a query key.
 * This should be used for global state (i.e. not coupled to a specific Materialize region.
 * See `buildRegionQueryKey` for additional details.
 */
export function buildGlobalQueryKey(scope: string) {
  return [buildQueryKeyPart(scope)] as const;
}

/**
 * Extracts the environmentVersion from a query key in a type-safe way.
 * @param queryKey - The query key array from React Query
 * @returns The environment version string if present, undefined otherwise
 */
export function extractEnvironmentVersion(
  queryKey: readonly unknown[],
): string | undefined {
  const firstPart = queryKey[0] as { environmentVersion?: string } | undefined;
  return firstPart?.environmentVersion;
}
