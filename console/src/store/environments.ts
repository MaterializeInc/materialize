// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";
import { useSuspenseQuery } from "@tanstack/react-query";
import { add, Duration, formatDuration, sub } from "date-fns";
import deepEqual from "fast-deep-equal";
import { atom, useAtom, useAtomValue } from "jotai";
import { atomFamily, loadable } from "jotai/utils";
import { sql } from "kysely";
import { PostgresError } from "pg-error-enum";
import React from "react";
import { gte as semverGte, parse as semverParse, SemVer } from "semver";

import { buildGlobalQueryKey } from "~/api/buildQueryKeySchema";
import { getRegion, RegionInfo } from "~/api/cloudRegionApi";
import {
  CATALOG_SERVER_CLUSTER,
  executeSql,
  queryBuilder,
} from "~/api/materialize";
import MaterializeErrorCode from "~/api/materialize/errorCodes";
import NetworkPolicyError from "~/api/materialize/NetworkPolicyError";
import { OpenApiFetchError } from "~/api/OpenApiFetchError";
import { AppConfig } from "~/config/AppConfig";
import { appConfigAtom } from "~/config/store";
import { getStore } from "~/jotai";
import {
  getRegionId,
  preferredCloudRegion,
  regionIdToSlug,
  SELECTED_REGION_KEY,
} from "~/store/cloudRegions";
import { assert, isApiError } from "~/util";
import { DbVersion, parseDbVersion } from "~/version/api";

import storageAvailable from "../utils/storageAvailable";
import { CloudRegion, cloudRegionsSelector } from "./cloudRegions";

/** Details about errors fetching environment health. */
export interface EnvironmentError {
  message: string;
  details?: OpenApiFetchError | Error;
}

/**
 * The health of an environment.
 *
 * pending - we have yet to run a health check, in practice this should never end up in
 * jotai state.
 * booting - the environment is created and assigned TLS certs, but environmentd is not
 * resolvable via DNS.
 * healthy - we have successfully executed a sql query against the environment.
 * crashed - no successful health check for a specified max duration since it was created.
 */
export type EnvironmentHealth = EnvironmentStatus["health"];

/**
 * The state of an environment. Only "enabled" and "creating" environments have a status property.
 *
 * disabled - the region is disabled, or we have never reached the region api (this it the default state)
 * unknown - we failed to get a response back
 * creating - we got a success response fom the enable region endpoint
 * enabled - the region is created, though it may not be healthy
 */
export type EnvironmentState = Environment["state"];

/** Represents an environment we didn't get a response for. */
export interface UnknownEnvironment {
  state: "unknown";
  errors: EnvironmentError[];
}

/** Represents an environment that is known to be disabled. */
export interface DisabledEnvironment {
  state: "disabled";
  errors: EnvironmentError[];
}

export interface HealthyStatus {
  health: "healthy";
  version: DbVersion;
  errors: EnvironmentError[];
}

export interface UnhealthyStatus {
  health: "crashed";
  errors: EnvironmentError[];
}

export interface BootingStatus {
  health: "booting";
  errors: EnvironmentError[];
}

export interface PendingStatus {
  health: "pending";
  errors: EnvironmentError[];
}

export interface BlockedStatus {
  health: "blocked";
  errors: EnvironmentError[];
}

export type EnvironmentStatus =
  | HealthyStatus
  | UnhealthyStatus
  | BootingStatus
  | PendingStatus
  | BlockedStatus;

/**
 * Represents an environment that is known to be starting up.
 *
 * Environments are in this state until their TLS certs are created, which can take quite
 * a while.
 */
export interface CreatingEnvironment {
  state: "creating";
  errors: EnvironmentError[];
}

/** Represents an environment that is known to exist. */
export interface EnabledEnvironment extends RegionInfo {
  state: "enabled";
  status: EnvironmentStatus;
  errors: EnvironmentError[];
}

export type Environment = UnknownEnvironment | LoadedEnvironment;

export type LoadedEnvironment =
  | DisabledEnvironment
  | CreatingEnvironment
  | EnabledEnvironment;

export const maybeEnvironmentForRegion = atomFamily(
  ({ regionId }: { regionId: string | undefined }) =>
    atom((get) => {
      if (regionId) {
        const environments = get(environmentsWithHealth);
        return environments?.get(regionId);
      } else {
        return undefined;
      }
    }),
  deepEqual,
);

export const fetchEnvironmentsWithHealth = async ({
  cloudRegions,
  appConfig,
}: {
  cloudRegions: Map<string, CloudRegion>;
  appConfig: AppConfig;
}) => {
  const result = new Map<string, Environment>();

  if (appConfig.mode === "self-managed") {
    const status = await fetchEnvironmentHealth({
      httpAddress: appConfig.environmentdConfig.environmentdHttpAddress,
      resolvable: true,
      enabledAt: fakeEnabledAt.toString(),
    });
    result.set(
      appConfig.environmentdConfig.regionId,
      buildFakeEnabledEnvironment({
        httpAddress: appConfig.environmentdConfig.environmentdHttpAddress,
        status,
      }),
    );
    return result;
  }

  if (appConfig.environmentdConfigOverride) {
    const status = await fetchEnvironmentHealth({
      httpAddress: appConfig.environmentdConfigOverride.environmentdHttpAddress,
      resolvable: true,
      enabledAt: fakeEnabledAt.toString(),
    });
    result.set(
      appConfig.environmentdConfigOverride.regionId,
      buildFakeEnabledEnvironment({
        httpAddress:
          appConfig.environmentdConfigOverride.environmentdHttpAddress,
        status,
      }),
    );
    return result;
  }

  for (const region of cloudRegions.values()) {
    const regionId = getRegionId(region);
    await Sentry.startSpan(
      {
        name: "fetch-environment-health",
        op: "http.client",
        attributes: { region: regionId },
      },
      async () => {
        try {
          const { data: regionResult } = await getRegion(region.regionApiUrl);
          const regionInfo =
            "regionInfo" in regionResult && regionResult.regionInfo;
          const regionState =
            "regionState" in regionResult && regionResult.regionState;
          if (regionState === "enablement-pending") {
            const envResult: CreatingEnvironment = {
              state: "creating",
              errors: [],
            };
            result.set(regionId, envResult);
          }
          if (
            regionState === "disabled" ||
            regionState === "deletion-pending" ||
            regionState === "soft-deleted"
          ) {
            const envResult: DisabledEnvironment = {
              state: "disabled",
              errors: [],
            };
            result.set(regionId, envResult);
          }
          if (regionInfo) {
            const envResult: EnabledEnvironment = {
              ...regionInfo,
              state: "enabled",
              status: { health: "pending", errors: [] },
              errors: [],
            };
            result.set(regionId, envResult);
          }
        } catch (error) {
          if (isApiError(error) && error.status === 404) {
            result.set(regionId, {
              state: "unknown",
              errors: [
                {
                  message: "Get region failed",
                  details: error as Error,
                },
              ],
            });
          } else {
            result.set(regionId, {
              state: "unknown",
              errors: [
                {
                  message: "Get region failed",
                  details: error as Error,
                },
              ],
            });
          }
        }
        const envResult = result.get(regionId);
        if (envResult && "httpAddress" in envResult && envResult.httpAddress) {
          const health = await fetchEnvironmentHealth(envResult);
          result.set(regionId, {
            ...envResult,
            status: health,
          });
        }
      },
    );
  }
  return result;
};

export type EnvironmentsWithHealth = Map<string, Environment>;
export const environmentsWithHealth = atom<EnvironmentsWithHealth | undefined>(
  undefined,
);

export const updateEnviromentState = (
  previous: Environment | undefined,
  next: Environment,
) => {
  if (!previous) return next;
  if (
    isEnvironmentReady(previous) &&
    next.state === "enabled" &&
    next.status.health === "booting"
  ) {
    // Error states during the `maxBootDuration` always come back as "booting", so if we
    // were previously healthy, ignore the error so we don't show the booting state again.
    return previous;
  }
  if (next.state === "unknown") {
    // If we get an unknown response, add the errors, but don't update the state, so we
    // can still make queries
    return {
      ...previous,
      errors: next.errors,
    };
  }
  return next;
};

export const environmentQueryKeys = {
  environmentHealth: (cloudRegions: Map<string, CloudRegion>) =>
    [buildGlobalQueryKey("environmentHealth"), cloudRegions] as const,
};

/**
 * Polls for region information and environment health.
 */
export const usePollEnvironmentHealth = (options: { intervalMs: number }) => {
  const [environmentMap, setValue] = useAtom(environmentsWithHealth);
  const [cloudRegions] = useAtom(cloudRegionsSelector);
  const appConfig = useAtomValue(appConfigAtom);

  // The environment objects are used in dependency arrays,
  // so the refrences need to be stable
  const updateValue = (newEnvMap: Map<string, Environment>) => {
    if (!environmentMap) {
      setValue(newEnvMap);
      return null;
    }
    let mapChanged = false;
    for (const [key, newValue] of newEnvMap.entries()) {
      const previousState = environmentMap.get(key);
      const merged = updateEnviromentState(previousState, newValue);
      newEnvMap.set(key, merged);
      if (!deepEqual(previousState, merged)) {
        mapChanged = true;
      }
    }
    if (mapChanged) {
      // Only update the state if something actually changed to prevent unnecessary
      // rerenders. React query does this automatically for some types, but not maps.
      setValue(newEnvMap);
    }
    return environmentMap;
  };

  useSuspenseQuery({
    queryKey: environmentQueryKeys.environmentHealth(cloudRegions),
    refetchInterval: options.intervalMs,
    queryFn: async ({ queryKey: [, regions] }) => {
      return Sentry.startSpan(
        {
          name: "poll-environment-health",
          op: "http.client",
          attributes: { polled: true },
        },
        async () => {
          const result = await fetchEnvironmentsWithHealth({
            cloudRegions: regions,
            appConfig,
          });
          return updateValue(result);
        },
      );
    },
  });
};

const loadableEnvironmentsWithHealth = loadable(environmentsWithHealth);
/**
 * Returns a map of regions and environment metadata.
 *
 * Throws a promise to trigger suspense if the data isn't available yet.
 */
export const useEnvironmentsWithHealth = () => {
  const [environments] = useAtom(environmentsWithHealth);

  if (environments) return environments;

  throw new Promise<EnvironmentsWithHealth>((resolve) => {
    const interval = setInterval(() => {
      const envLoadable = getStore().get(loadableEnvironmentsWithHealth);
      const map =
        envLoadable.state === "hasData" ? envLoadable.data : undefined;
      if (map) {
        clearInterval(interval);
        resolve(map);
      }
    }, 50);
  });
};

const defaultTimeout = 10_000; // 10 seconds
const maxBootDuration = { minutes: 15 };
// A made up enabled at time only used during impersonation and self-managed,
// since we don't know when the environment was enabled.
const fakeEnabledAt = sub(new Date(), {
  minutes: maxBootDuration.minutes + 1,
});

/**
 * Builds a fake enabled environment for self-managed and impersonation.
 */

function buildFakeEnabledEnvironment({
  httpAddress,
  status,
}: {
  httpAddress: string;
  status: EnvironmentStatus;
}): EnabledEnvironment {
  return {
    httpAddress,
    sqlAddress: "",
    resolvable: true,
    // A made up enabled at time only used during impersonation and self-managed,
    // since we don't know when the environment was enabled.
    enabledAt: fakeEnabledAt.toString(),
    state: "enabled",
    status,
    errors: [],
  };
}

const selectVersionQuery = sql`SELECT mz_version()`.compile(queryBuilder);

const BLOCKED_ERROR_CODES: Set<MaterializeErrorCode | PostgresError> = new Set([
  MaterializeErrorCode.ORGANIZATION_BLOCKED,
  MaterializeErrorCode.NETWORK_POLICY_SESSION_DENIED,
]);

export const fetchEnvironmentHealth = async (
  environment: {
    httpAddress: EnabledEnvironment["httpAddress"];
    resolvable: EnabledEnvironment["resolvable"];
    enabledAt: EnabledEnvironment["enabledAt"];
  },
  timeoutMs: number = defaultTimeout,
  maxBoot: Duration = maxBootDuration,
): Promise<EnvironmentStatus> => {
  // Determine if the environment is healthy by issuing a basic SQL query.
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  let version: DbVersion | undefined = undefined;
  try {
    if (!environment.resolvable) {
      throw new Error(`Environment unresolvable`);
    }
    const result = await executeSql(
      environment.httpAddress,
      {
        queries: [
          {
            query: selectVersionQuery.sql,
            params: selectVersionQuery.parameters as string[],
          },
        ],
        cluster: CATALOG_SERVER_CLUSTER,
      },
      { signal: controller.signal },
    );
    if ("errorMessage" in result) {
      const errors: EnvironmentError[] = [];
      if ("code" in result && BLOCKED_ERROR_CODES.has(result.code)) {
        if (
          result.code === MaterializeErrorCode.NETWORK_POLICY_SESSION_DENIED
        ) {
          errors.push({
            message: "Environment blocked",
            details: new NetworkPolicyError(result.code, result.detail),
          });
        }
        return {
          health: "blocked",
          errors,
        };
      }
      errors.push({
        message: "Environmentd health check failed",
      });
      errors.push({
        message: result.errorMessage,
      });
      return { health: "crashed", errors };
    } else {
      const versionString = result.results[0].rows[0][0] as string;
      version = parseDbVersion(versionString);
      return { health: "healthy", version, errors: [] };
    }
  } catch (e) {
    const enabledAt = new Date(environment.enabledAt);
    const cutoff = add(enabledAt, maxBoot);
    if (new Date() > cutoff) {
      const errors: EnvironmentError[] = [];
      errors.push({
        message: `Environment not resolvable for more than ${formatDuration(
          maxBoot,
        )} after creation`,
        details: e as Error,
      });
      return { health: "crashed", errors };
    } else {
      return { health: "booting", errors: [] };
    }
  } finally {
    clearTimeout(timeout);
  }
};

export const environmentErrors = (env: Environment): EnvironmentError[] => {
  switch (env.state) {
    case "disabled":
      return env.errors;
    case "enabled":
      switch (env.status.health) {
        case "crashed":
          return env.status.errors;
      }
  }
  return [];
};

const MAX_INITIAL_HEALTH_CHECK_WAIT_MS = 10_000;
const INITIAL_HEALTHCHECK_INTERVAL_MS = 10;
/**
 * Waits for initial health checks, then returns current environments with health.
 */
const environmentsWithHealthLoaded = atom(async (get) => {
  const environments = get(environmentsWithHealth);
  if (environments) return environments;

  return new Promise<EnvironmentsWithHealth>((resolve, reject) => {
    let waited = 0;
    const timeout = setInterval(() => {
      if (waited > MAX_INITIAL_HEALTH_CHECK_WAIT_MS) {
        reject(
          `Failed to get environmentsWithHealth for ${
            MAX_INITIAL_HEALTH_CHECK_WAIT_MS / 1000
          } seconds`,
        );
        clearInterval(timeout);
      }
      const envs = getStore().get(environmentsWithHealth);
      if (envs) {
        clearInterval(timeout);
        resolve(envs);
      } else {
        waited += INITIAL_HEALTHCHECK_INTERVAL_MS;
      }
    }, INITIAL_HEALTHCHECK_INTERVAL_MS);
  });
});

/** Finds the default region based their stored preference or the first enabled region. */
export const defaultRegionSelector = atom(async (get) => {
  const appConfig = get(appConfigAtom);

  if (appConfig.mode === "self-managed") {
    return appConfig.environmentdConfig.regionId;
  }

  if (appConfig.environmentdConfigOverride) {
    return appConfig.environmentdConfigOverride.regionId;
  }

  const cloudRegions = await get(cloudRegionsSelector);
  const environments = await get(environmentsWithHealthLoaded);
  const preferredRegion = await get(preferredCloudRegion);
  if (preferredRegion) return preferredRegion;

  // If they don't have a preference set, default to the first enabled region
  // and set that as the preference for future visits
  for (const [regionId, env] of environments.entries()) {
    if (env.state === "enabled") {
      if (storageAvailable("localStorage")) {
        window.localStorage.setItem(SELECTED_REGION_KEY, regionId);
      }
      return regionId;
    }
  }
  return cloudRegions.keys().next().value as string;
});

/**
 * This is a private atom that only exists because you cannot initialize an atom with an
 * async value.
 */
const _currentRegionIdAtom = atom<string | null>(null);

/**
 * Synchronous read-only atom for the current region ID.
 * Use this for store.sub() subscriptions which require a sync atom.
 * Returns null before initialization (unlike currentRegionIdAtom which falls back to default).
 */
export const currentRegionIdSyncAtom = atom((get) => get(_currentRegionIdAtom));

/** The ID of the currently selected region. */
export const currentRegionIdAtom = atom(
  async (get) => {
    const region = get(_currentRegionIdAtom);
    if (region) return region;
    return get(defaultRegionSelector);
  },
  (_get, set, newRegionId: string) => {
    set(_currentRegionIdAtom, newRegionId);
    if (storageAvailable("localStorage")) {
      window.localStorage.setItem(SELECTED_REGION_KEY, newRegionId);
    }
  },
);

export const currentRegionIdAtomLoadable = loadable(currentRegionIdAtom);

/** The state for the currently selected environment. */
export const currentEnvironmentState = atom(async (get) => {
  const currentRegionId = await get(currentRegionIdAtom);
  const envs = get(environmentsWithHealth);
  if (!envs || !currentRegionId) return undefined;
  return envs.get(currentRegionId);
});

/** The current environment's http address */
export function useCurrentEnvironmentHttpAddress() {
  const currentEnvironment = useAtomValue(currentEnvironmentState);
  const httpAddress = React.useMemo(
    () =>
      currentEnvironment?.state === "enabled"
        ? currentEnvironment.httpAddress
        : "",
    [currentEnvironment],
  );
  return httpAddress;
}

export const useRegionSlug = () => {
  const [currentRegionId] = useAtom(currentRegionIdAtom);
  if (!currentRegionId) {
    throw new Error("currentRegionId not set.");
  }
  return regionIdToSlug(currentRegionId);
};

/**
 * Gate code on the current version being greater than or equal to a specified version.
 * The tri-state return-value is true if the current environment version
 * is greater than or equal to the supplied version,
 * false if it is less than the supplied version, and undefined
 * if it can't be found (because there is no current environment or because the current
 * environment is unhealthy).
 *
 * It is recommended to call this function with pre-release semver strings. For example,
 * `useEnvironmentGate("0.55.0-dev")` will return true on v0.55.x and their pre-release builds,
 * but false on v0.54.x
 */
export const useEnvironmentGate = (
  version: string | SemVer,
): boolean | null => {
  const suppliedVersion = semverParse(version);
  assert(suppliedVersion);
  const [environment] = useAtom(currentEnvironmentState);
  if (
    environment &&
    environment.state === "enabled" &&
    environment.status.health === "healthy"
  ) {
    const actualVersion = environment.status.version;
    return semverGte(actualVersion.crateVersion, suppliedVersion);
  }
  return null;
};

/** The number of enabled environments. */
export const numEnabledEnvironmentsState = atom<number | undefined>((get) => {
  const envs = get(environmentsWithHealth);
  if (!envs) return undefined;
  return Array.from(envs.values()).filter((env) => env.state === "enabled")
    .length;
});

/** The number of environments that are not disabled. */
export const numTotalEnvironmentsState = atom<number | undefined>((get) => {
  const envs = get(environmentsWithHealth);
  if (!envs) return undefined;
  // Include `unknown` here as this is used to limit how many concurrent
  // environments a customer may have
  return Array.from(envs.values()).filter((env) => env.state !== "disabled")
    .length;
});

export function isEnvironmentReady(environment?: Environment) {
  if (!environment) {
    return false;
  }

  return !(
    environment.state === "disabled" ||
    environment.state === "creating" ||
    environment.state === "unknown" ||
    (environment.state === "enabled" &&
      (environment.status.health === "booting" ||
        environment.status.health === "pending" ||
        environment.status.health === "blocked"))
  );
}
