// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { useSegmentPageTracking } from "~/analytics/segment";
import useHubspotNpsSurvey from "~/analytics/useHubspotNpsSurvey";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import { User } from "~/external-library-wrappers/frontegg";
import useIntercom from "~/hooks/useIntercom";
import { usePrivileges } from "~/hooks/usePrivileges";
import { useSetInitialRegion } from "~/hooks/useSetInitialRegion";
import { useShowEnvironmentErrors } from "~/hooks/useShowEnvironmentErrors";
import { useSentryIdentifyOrganization } from "~/sentry";
import { useSubscribeToAllClusters } from "~/store/allClusters";
import { useSubscribeToAllObjects } from "~/store/allObjects";
import { useSubscribeToAllSchemas } from "~/store/allSchemas";
import { usePollEnvironmentHealth } from "~/store/environments";
import { useTrackFocus } from "~/store/focus";

import IncidentStatusWidget from "./IncidentStatusWidget/IncidentStatusWidget";

/** Initializes global features, rendered after frontegg token is available if Cloud */
const useAppInitializer = () => {
  useTrackFocus();
  usePollEnvironmentHealth({ intervalMs: 5000 });
  useShowEnvironmentErrors();
  useSetInitialRegion();
  usePrivileges();
  useSubscribeToAllObjects();
  useSubscribeToAllSchemas();
  useSubscribeToAllClusters();

  return null;
};

export const CloudAppInitializer = ({ user }: { user: User }) => {
  useAppInitializer();
  useHubspotNpsSurvey({ user });
  useSegmentPageTracking({ user });
  useSentryIdentifyOrganization({ user });
  useIntercom();
  return <IncidentStatusWidget />;
};

export const CloudImpersonationAppInitializer = () => {
  useAppInitializer();
  return null;
};

export const FlexibleDeploymentAppInitializer = () => {
  useAppInitializer();
  return null;
};

export const AppInitializer = () => {
  return (
    <AppConfigSwitch
      cloudConfigElement={({ runtimeConfig }) =>
        runtimeConfig.isImpersonating ? (
          <CloudImpersonationAppInitializer />
        ) : (
          <CloudAppInitializer user={runtimeConfig.user} />
        )
      }
      selfManagedConfigElement={<FlexibleDeploymentAppInitializer />}
    />
  );
};
