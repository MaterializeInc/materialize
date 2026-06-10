// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * @module
 * API authentication support.
 */

import { useQuery } from "@tanstack/react-query";

import { CloudRuntimeConfig } from "~/config/AppConfigSwitch";
import { useAppConfig } from "~/config/useAppConfig";
import {
  type ITenantsResponse,
  type User,
} from "~/external-library-wrappers/frontegg";
import { useFlags } from "~/hooks/useFlags";

import { buildGlobalQueryKey } from "./buildQueryKeySchema";
import {
  getCurrentOrganization,
  getSelfManagedSubscription,
  issueIntercomJwt,
  Organization,
} from "./cloudGlobalApi";
import { OpenApiRequestOptions } from "./types";

export const ADMIN_ROLE_KEY = "MaterializePlatformAdmin";

export const LOCAL_IMPERSONATION_ORGANIZATION_STUB: Organization = {
  id: "local-impersonation-organization-id",
  name: "Local Impersonation Organization",
  blocked: false,
  onboarded: true,
  subscription: null,
  trialExpiresAt: null,
};

export const queryKeys = {
  currentOrganization: () => [
    buildGlobalQueryKey("global-api"),
    "current-organization",
  ],
  intercomJwt: () => [buildGlobalQueryKey("global-api"), "intercom-jwt"],
  selfManagedSubscription: () => [
    buildGlobalQueryKey("global-api"),
    "self-managed-subscription",
  ],
};

function hasPermission(user: User | null | undefined, key: string): boolean {
  return !!user && !!user.permissions.find((p) => p.key === key);
}

export function hasEnvironmentReadPermission(user: User | null): boolean {
  return hasPermission(user, "materialize.environment.read");
}

export function hasEnvironmentWritePermission(user: User | null): boolean {
  return hasPermission(user, "materialize.environment.write");
}

export function hasTenantApiTokenPermissions(user: User | null): boolean {
  return [
    "fe.secure.read.tenantApiTokens",
    "fe.secure.write.tenantApiTokens",
    "fe.secure.delete.tenantApiTokens",
  ].every((permission) => hasPermission(user, permission));
}

export function hasInvoiceReadPermission(
  user: User | null | undefined,
): boolean {
  return hasPermission(user, "materialize.invoice.read");
}

export function isSuperUser(user: User | null): boolean {
  return !!user && Boolean(user.roles.find((r) => r.key === ADMIN_ROLE_KEY));
}

export function getCurrentTenant(
  user: User,
  tenants: ITenantsResponse[],
): ITenantsResponse {
  const tenant = tenants.find((t) => t.tenantId === user.tenantId);
  if (!tenant) {
    throw new Error(`Unknown tenant: ${user.tenantId}`);
  }
  return tenant;
}

export async function fetchCurrentOrganization({
  signal,
}: { signal?: OpenApiRequestOptions["signal"] } = {}) {
  const result = await getCurrentOrganization({ signal });
  return result.data;
}

export async function fetchIntercomJwt({
  signal,
}: { signal?: OpenApiRequestOptions["signal"] } = {}) {
  const result = await issueIntercomJwt({ signal });
  return result.data;
}

export async function fetchSelfManagedSubscription({
  signal,
}: { signal?: OpenApiRequestOptions["signal"] } = {}) {
  const result = await getSelfManagedSubscription(null, { signal });
  return result.data;
}

export function useCurrentOrganization(
  {
    refetchInterval,
  }: {
    refetchInterval?: number;
  } = {
    refetchInterval: undefined,
  },
) {
  // For impersonation for local development, we disable calls to the global API
  // and return a stub organization.
  const appConfig = useAppConfig();
  const isLocalImpersonation =
    appConfig.mode === "cloud" &&
    appConfig.isImpersonating &&
    appConfig.isLocalImpersonation;

  const {
    data: organization,
    error,
    isLoading: loading,
  } = useQuery({
    queryKey: queryKeys.currentOrganization(),
    queryFn: fetchCurrentOrganization,
    refetchInterval,
    enabled: !isLocalImpersonation,
  });

  if (isLocalImpersonation) {
    return {
      organization: LOCAL_IMPERSONATION_ORGANIZATION_STUB,
      loading: false,
      error: null,
    };
  }

  return { organization, loading, error };
}

export function useSelfManagedSubscription() {
  const {
    data: subscription,
    error,
    isError,
    isLoading,
  } = useQuery({
    queryKey: queryKeys.selfManagedSubscription(),
    queryFn: fetchSelfManagedSubscription,
    staleTime: Infinity,
  });

  return { subscription, isLoading, isError, error };
}

// Convenience hook for getting the current organization ID based on
// the app's deployment mode. Although "organizations" don't really exist in
// self managed environments, a lot of our code uses the organization ID as part of a key to cache
// items in local storage. Thus we use this hook out of convenience and return null
// in deployment modes where the organization ID is not available.
export function useMaybeCurrentOrganizationId() {
  const appConfig = useAppConfig();
  const isLocalImpersonation =
    appConfig.mode === "cloud" &&
    appConfig.isImpersonating &&
    appConfig.isLocalImpersonation;

  const isCurrentOrganizationFetchEnabled =
    appConfig.mode !== "self-managed" && !isLocalImpersonation;

  const res = useQuery({
    queryKey: queryKeys.currentOrganization(),
    queryFn: fetchCurrentOrganization,
    enabled: isCurrentOrganizationFetchEnabled,
    select: (data) => data.id,
    // We don't expect the organization ID to change and we already reset the
    // query cache on organization change.
    staleTime: Infinity,
  });

  if (!isCurrentOrganizationFetchEnabled) {
    return null;
  }

  return res;
}

export function useIntercomJwt() {
  const {
    data: intercomJwt,
    error,
    isLoading: loading,
  } = useQuery({
    queryKey: queryKeys.intercomJwt(),
    queryFn: fetchIntercomJwt,
    staleTime: Infinity,
    gcTime: Infinity,
  });

  return { intercomJwt, loading, error };
}

export function useCanViewBilling({
  runtimeConfig,
}: {
  runtimeConfig: CloudRuntimeConfig;
}) {
  const flags = useFlags();
  // We assume impersonation users shouldn't access the billing management page
  if (runtimeConfig.isImpersonating) {
    return false;
  }

  const isOrgAdmin = hasEnvironmentWritePermission(runtimeConfig.user);

  return !!flags["billing-ui-3756"] && isOrgAdmin;
}

export function useCanViewUsage({
  runtimeConfig,
}: {
  runtimeConfig: CloudRuntimeConfig;
}) {
  const { organization } = useCurrentOrganization();
  const isBillingVisible = useCanViewBilling({ runtimeConfig });

  // We assume impersonation users can always view usage pages
  if (runtimeConfig.isImpersonating) {
    return true;
  }

  const allowedPlanTypes = ["capacity", "on-demand"];
  if (isBillingVisible) {
    allowedPlanTypes.push("evaluation");
  }

  return (
    hasInvoiceReadPermission(runtimeConfig.user) &&
    !!organization?.subscription?.type &&
    allowedPlanTypes.includes(organization.subscription.type)
  );
}
