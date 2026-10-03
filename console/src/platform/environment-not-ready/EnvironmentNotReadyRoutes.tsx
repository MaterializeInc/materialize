// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Text, VStack } from "@chakra-ui/react";
import { useAtomValue } from "jotai";
import React from "react";
import { Link, Navigate, Route } from "react-router-dom";

import TextLink from "~/components/TextLink";
import { User } from "~/external-library-wrappers/frontegg";
import { useToast } from "~/hooks/useToast";
import { BaseLayout } from "~/layouts/BaseLayout";
import { regionPath } from "~/platform/routeHelpers";
import { SentryRoutes } from "~/sentry";
import {
  currentRegionIdAtom,
  useEnvironmentsWithHealth,
  useRegionSlug,
} from "~/store/environments";

import EnableRegion from "./EnableRegion";
import { OnboardingSteps } from "./OnboardingSteps";

const REGION_READY_TOAST_ID = "region-ready-toast";

export const RegionReadyToastBody = (props: {
  currentRegionId: string;
  regionPath: string;
}) => {
  return (
    <VStack>
      <Text textStyle="text-ui-med">{props.currentRegionId} is ready!</Text>
      <TextLink textStyle="text-small" as={Link} to={props.regionPath}>
        Go to Materialize Console &rarr;
      </TextLink>
    </VStack>
  );
};

/**
 * Pops a toast when the current region becomes healthy while the user is
 * still in the environment-not-ready flow. Renders nothing.
 */
const RegionReadyToast = () => {
  const toast = useToast();
  const regionSlug = useRegionSlug();
  const environments = useEnvironmentsWithHealth();
  const currentRegionId = useAtomValue(currentRegionIdAtom);
  const currentEnvironment = environments.get(currentRegionId);

  // The toast reference isn't stable, so the unmount cleanup reads it through
  // a ref instead of depending on it directly.
  const toastRef = React.useRef(toast);
  React.useEffect(() => {
    toastRef.current = toast;
  }, [toast]);

  React.useEffect(() => {
    if (
      currentEnvironment &&
      currentEnvironment.state === "enabled" &&
      currentEnvironment.status.health === "healthy" &&
      !toast.isActive(REGION_READY_TOAST_ID)
    ) {
      toast({
        id: REGION_READY_TOAST_ID,
        duration: null, // keep it open
        position: "top-right",
        description: (
          <RegionReadyToastBody
            currentRegionId={currentRegionId}
            regionPath={regionPath(regionSlug)}
          />
        ),
      });
    }
  }, [currentEnvironment, currentRegionId, regionSlug, toast]);

  React.useEffect(() => {
    return () => {
      // Close the toast when this component unmounts
      toastRef.current.close(REGION_READY_TOAST_ID);
    };
  }, []);

  return null;
};

export const EnvironmentNotReadyRoutes = ({ user }: { user: User }) => {
  return (
    // The welcome dialog is suppressed here because this flow has its own
    // region-ready affordances (the toast and the tutorial's "Open console"
    // button), which the dialog would cover.
    <BaseLayout hideWelcomeDialog accountOnlyNav>
      <RegionReadyToast />
      <SentryRoutes>
        <Route path="enable-region" element={<EnableRegion user={user} />} />
        <Route path=":step" element={<OnboardingSteps user={user} />} />
        <Route path="*" element={<Navigate to="../enable-region" replace />} />
      </SentryRoutes>
    </BaseLayout>
  );
};
