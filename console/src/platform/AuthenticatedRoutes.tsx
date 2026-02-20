// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useAtom, useAtomValue } from "jotai";
import React from "react";
import {
  Navigate,
  Route,
  useLocation,
  useNavigate,
  useParams,
} from "react-router-dom";

import { AppPasswordRoutes } from "~/access/AppPasswordRoutes";
import { LicenseRoutes } from "~/access/license/LicenseRoutes";
import {
  hasInvoiceReadPermission,
  useMaybeCurrentOrganizationId,
} from "~/api/auth";
import { AppInitializer } from "~/components/AppInitializer";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import { getIsFronteggRoute } from "~/fronteggRoutes";
import { useFlags } from "~/hooks/useFlags";
import { BaseLayout } from "~/layouts/BaseLayout";
import { ShellLayout } from "~/layouts/ShellLayout";
import ClusterRoutes from "~/platform/clusters/ClusterRoutes";
import BlockedState from "~/platform/environment-not-ready/BlockedState";
import { EnvironmentNotReadyRoutes } from "~/platform/environment-not-ready/EnvironmentNotReadyRoutes";
import { EnvironmentOverviewRoutes } from "~/platform/environment-overview/EnvironmentOverviewRoutes";
import IntegrationsRoutes from "~/platform/integrations/IntegrationsRoutes";
import { ObjectExplorerDetailRoutes } from "~/platform/object-explorer/ObjectExplorerDetailRoutes";
import { ObjectExplorerRoutes } from "~/platform/object-explorer/ObjectExplorerRoutes";
import QueryHistoryRoutes from "~/platform/query-history/QueryHistoryRoutes";
import RolesRoutes from "~/platform/roles/RolesRoutes";
import {
  environmentNotReadyPath,
  SHELL_SLUG as HOME_PAGE_SLUG,
  shellPath as homePagePath,
} from "~/platform/routeHelpers";
import ShellRoutes from "~/platform/shell/ShellRoutes";
import { ShellWebsocketProvider } from "~/platform/shell/ShellWebsocketProvider";
import SourceRoutes from "~/platform/sources/SourceRoutes";
import { SentryRoutes } from "~/sentry";
import { regionIdToSlug, useRegionSlugToId } from "~/store/cloudRegions";
import {
  currentEnvironmentState,
  currentRegionIdAtom,
  defaultRegionSelector,
  isEnvironmentReady,
  useEnvironmentsWithHealth,
  useRegionSlug,
} from "~/store/environments";
import { isCurrentOrganizationBlockedAtom } from "~/store/organization";
import { assert } from "~/util";

import UsageRoutes from "./billing/UsageRoutes";
import SinksList from "./sinks/SinksList";

const InternalRoutes = React.lazy(
  () => import("~/platform/internal/InternalRoutes"),
);

const UsageRoutesWrapper = () => {
  return (
    <AppConfigSwitch
      cloudConfigElement={({ runtimeConfig, appConfig }) => {
        // For impersonation, we don't have access to the user object and don't care about read permission.
        if (
          !runtimeConfig.isImpersonating &&
          !hasInvoiceReadPermission(runtimeConfig.user)
        ) {
          return <RedirectToHome />;
        }
        return (
          <BaseLayout>
            <UsageRoutes
              runtimeConfig={runtimeConfig}
              stripePromise={appConfig.stripePromise}
            />
          </BaseLayout>
        );
      }}
      selfManagedConfigElement={<RedirectToHome />}
    />
  );
};

const EnvironmentNotReadyRoutesWrapper = () => {
  return (
    <AppConfigSwitch
      cloudConfigElement={({ runtimeConfig }) => {
        if (runtimeConfig.isImpersonating) {
          return <RedirectToHome />;
        }
        return (
          <RestrictIfBlocked>
            <EnvironmentNotReadyRoutes user={runtimeConfig.user} />
          </RestrictIfBlocked>
        );
      }}
      selfManagedConfigElement={<RedirectToHome />}
    />
  );
};

const NavigatePreservingSearch = ({ to }: { to: string }) => {
  const { search } = useLocation();
  return <Navigate to={{ pathname: to, search }} replace />;
};

export const AuthenticatedRoutes = () => {
  const flags = useFlags();
  const isLicenseKeysEnabled = flags["license-keys-3833"];
  const isRbacEnabled = flags["rbac-ui-9904"];

  return (
    <>
      <AppInitializer />
      <SentryRoutes>
        <Route
          path="/regions/:regionSlug/*"
          element={
            <RestrictIfBlocked>
              <EnvironmentRoutes />
            </RestrictIfBlocked>
          }
        />
        <Route
          path="/access"
          element={<Navigate to="/access/app-passwords" replace />}
        />
        {/* Create a redirect, preserving the search params for the `mz` CLI */}
        <Route
          path="/access/cli"
          element={<NavigatePreservingSearch to="/access/app-passwords/cli" />}
        />
        <Route
          path="/access/app-passwords/*"
          element={
            <AppConfigSwitch
              cloudConfigElement={({ runtimeConfig }) =>
                !runtimeConfig.isImpersonating ? (
                  <BaseLayout>
                    <AppPasswordRoutes user={runtimeConfig.user} />
                  </BaseLayout>
                ) : (
                  <RedirectToHome />
                )
              }
              selfManagedConfigElement={<RedirectToHome />}
            />
          }
        />

        {isLicenseKeysEnabled && (
          <Route path="license">
            <Route
              index
              path="*"
              element={
                <BaseLayout>
                  <LicenseRoutes />
                </BaseLayout>
              }
            />
          </Route>
        )}
        {isRbacEnabled && (
          <Route path="roles">
            <Route
              index
              path="*"
              element={
                <BaseLayout>
                  <RolesRoutes />
                </BaseLayout>
              }
            />
          </Route>
        )}
        <Route path={environmentNotReadyPath}>
          <Route
            index
            path="*"
            element={<EnvironmentNotReadyRoutesWrapper />}
          />
        </Route>
        <Route path="usage">
          <Route index path="*" element={<UsageRoutesWrapper />} />
        </Route>
        {flags["internal-apps"] && (
          <Route path="internal">
            <Route index path="*" element={<InternalRoutes />} />
          </Route>
        )}
        <Route path="*" element={<RedirectToHome />} />
      </SentryRoutes>
    </>
  );
};

type RegionParams = "regionSlug";

const RestrictIfBlocked = ({ children }: React.PropsWithChildren) => {
  const isCurrentOrganizationBlocked = useAtomValue(
    isCurrentOrganizationBlockedAtom,
  );
  if (isCurrentOrganizationBlocked) {
    return (
      <BaseLayout>
        <BlockedState />
      </BaseLayout>
    );
  }
  return children;
};

const EnvironmentRoutes = () => {
  const flags = useFlags();
  const params = useParams<RegionParams>();
  const maybeOrganizationId = useMaybeCurrentOrganizationId();

  const navigate = useNavigate();
  const environments = useEnvironmentsWithHealth();
  const [currentRegionId, setCurrentRegionId] = useAtom(currentRegionIdAtom);
  const [defaultRegion] = useAtom(defaultRegionSelector);
  assert(params.regionSlug);
  const regionId = useRegionSlugToId(params.regionSlug);

  React.useEffect(() => {
    if (regionId && !isEnvironmentReady(environments.get(regionId))) {
      assert(params.regionSlug);
      navigate(environmentNotReadyPath, { replace: true });
    }
  }, [environments, navigate, params.regionSlug, regionId]);

  React.useEffect(() => {
    if (!regionId) {
      navigate(`/regions/${regionIdToSlug(defaultRegion)}`);
      return;
    }

    if (currentRegionId !== regionId) {
      // Synchronize the url with jotai, this happens on navigation to a link to another cluster or back navigation
      setCurrentRegionId(regionId);
    }
  }, [
    defaultRegion,
    currentRegionId,
    navigate,
    params.regionSlug,
    regionId,
    setCurrentRegionId,
  ]);

  const isOrganizationIdLoading =
    maybeOrganizationId !== null && maybeOrganizationId.isLoading;

  if (!regionId || isOrganizationIdLoading) {
    return null;
  }

  const organizationId =
    maybeOrganizationId !== null ? maybeOrganizationId.data : undefined;

  return (
    <ShellWebsocketProvider regionId={regionId} organizationId={organizationId}>
      <SentryRoutes>
        <Route
          path={HOME_PAGE_SLUG}
          element={
            <BaseLayout>
              <ShellLayout>
                <ShellRoutes />
              </ShellLayout>
            </BaseLayout>
          }
        />
        <Route path="objects">
          <Route
            index
            path="*"
            element={
              <BaseLayout sectionNav={<ObjectExplorerRoutes />}>
                <ObjectExplorerDetailRoutes />
              </BaseLayout>
            }
          />
        </Route>
        <Route path="clusters">
          <Route
            index
            path="*"
            element={
              <BaseLayout>
                <ClusterRoutes />
              </BaseLayout>
            }
          />
        </Route>
        <Route path="sources">
          <Route
            index
            path="*"
            element={
              <BaseLayout>
                <SourceRoutes />
              </BaseLayout>
            }
          />
        </Route>
        <Route
          path="sinks"
          element={
            <BaseLayout>
              <SinksList />
            </BaseLayout>
          }
        />
        {flags["environment-overview-2855"] && (
          <Route
            path="environment-overview"
            element={
              <BaseLayout>
                <EnvironmentOverviewRoutes />
              </BaseLayout>
            }
          />
        )}

        <Route path="query-history">
          <Route
            index
            path="*"
            element={
              <AppConfigSwitch
                cloudConfigElement={
                  <BaseLayout>
                    <QueryHistoryRoutes />
                  </BaseLayout>
                }
                // TODO (SangJunBak): Remove guard once we want to re-enable Query History for self managed https://github.com/MaterializeInc/cloud/issues/10755
                selfManagedConfigElement={<RedirectToHome />}
              />
            }
          />
        </Route>
        <Route path="integrations">
          <Route
            index
            path="*"
            element={
              <BaseLayout>
                <IntegrationsRoutes />
              </BaseLayout>
            }
          />
        </Route>
        <Route
          path="*"
          element={<Navigate to={homePagePath(params.regionSlug)} replace />}
        />
      </SentryRoutes>
    </ShellWebsocketProvider>
  );
};

const RedirectToHome = () => {
  const location = useLocation();
  useEnvironmentsWithHealth();
  const regionSlug = useRegionSlug();
  const navigate = useNavigate();
  const [currentEnvironment] = useAtom(currentEnvironmentState);

  // For Frontegg routes, we return null and let Frontegg handle it.
  const isFronteggRoute = getIsFronteggRoute(location.pathname);

  React.useEffect(() => {
    if (!isFronteggRoute) {
      /*
       * Note: It's important to use `useNavigate` rather than the `<Navigate>` component since sometimes this flow can happen:
       * 1. Frontegg redirects to the root path
       * 2. We redirect
       * 3. Frontegg redirects to root path again before we can unmount RedirectToHome
       * 4. Navigate gets rendered instead of re-mounted, which doesn't trigger a redirect
       */
      if (!currentEnvironment || !isEnvironmentReady(currentEnvironment)) {
        navigate(environmentNotReadyPath, { replace: true });
      } else {
        navigate(homePagePath(regionSlug), { replace: true });
      }
    }
  });

  return null;
};
