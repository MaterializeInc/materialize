// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Spinner, Text, useTheme, VStack } from "@chakra-ui/react";
import { useAtom, useAtomValue } from "jotai";
import React from "react";
import { Navigate } from "react-router-dom";

import ErrorBox from "~/components/ErrorBox";
import { RegionSelectorForm } from "~/components/RegionSelectorForm";
import { User } from "~/external-library-wrappers/frontegg";
import CreateEnvironmentButton from "~/platform/environment-not-ready/CreateEnvironmentButton";
import useCreateEnvironment from "~/platform/environment-not-ready/useCreateEnvironment";
import { homePagePath } from "~/platform/routeHelpers";
import { cloudRegionsSelector, regionIdToSlug } from "~/store/cloudRegions";
import {
  currentRegionIdAtom,
  useEnvironmentsWithHealth,
} from "~/store/environments";
import { isCurrentOrganizationBlockedAtom } from "~/store/organization";
import { MaterializeTheme } from "~/theme";

import ContactSalesCta from "./ContactSalesCta";
import CreateEnvironmentWarning from "./CreateEnvironmentWarning";

const LoadingState = () => (
  <HStack width="100%" justifyContent="center">
    <Spinner />
  </HStack>
);

const EnableRegionContent = ({ user }: { user: User }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const environments = useEnvironmentsWithHealth();
  const { creatingRegionId, createRegion } = useCreateEnvironment();
  const [currentRegionId] = useAtom(currentRegionIdAtom);
  const [cloudRegions] = useAtom(cloudRegionsSelector);
  const isOrganizationBlocked = useAtomValue(isCurrentOrganizationBlockedAtom);

  const availableRegions = Array.from(cloudRegions.keys());

  const [selectedRegion, setSelectedRegion] = React.useState<string | null>(
    null,
  );

  if (Array.from(environments.values()).every((e) => e.state === "disabled")) {
    return (
      <VStack
        spacing={10}
        alignItems="flex-start"
        width={{ sm: "80%", md: "500px" }}
      >
        <VStack spacing={4} align="flex-start">
          <Text as="h1" textStyle="heading-lg">
            Welcome to Materialize
          </Text>
          <Text
            as="h4"
            textStyle="text-base"
            fontSize="16px"
            color={colors.foreground.secondary}
          >
            Where would you like to run your Materialize environment?
          </Text>
        </VStack>
        <VStack spacing={8} alignItems="flex-start" width="100%">
          <RegionSelectorForm
            options={availableRegions}
            onChange={setSelectedRegion}
          />
          <HStack
            alignItems="flex-end"
            justifyContent="space-between"
            width="100%"
          >
            <ContactSalesCta />
            <CreateEnvironmentButton
              regionId={selectedRegion}
              createRegion={createRegion}
              creatingRegionId={creatingRegionId}
              tenantIsBlocked={isOrganizationBlocked}
              size="md"
              variant="primary"
              user={user}
            />
          </HStack>
        </VStack>
      </VStack>
    );
  } else {
    const currentEnvironment = environments.get(currentRegionId)!;

    switch (currentEnvironment.state) {
      case "unknown":
        return (
          <ErrorBox message="We are having trouble connecting to your Materialize region" />
        );
      case "creating":
        return <Navigate to="../creating-environment" />;
      case "enabled":
        switch (currentEnvironment.status.health) {
          case "pending":
            return <LoadingState />;
          case "booting":
            return <Navigate to="../creating-environment" />;
          case "blocked":
          case "crashed":
          case "healthy":
            return (
              <Navigate
                to={homePagePath(regionIdToSlug(currentRegionId))}
                replace
              />
            );
          default:
            return <LoadingState />;
        }
      case "disabled":
        return (
          <VStack spacing={4} align="flex-start" maxWidth="500px">
            <Text as="h1" textStyle="heading-lg">
              {currentRegionId} disabled
            </Text>
            <Text
              as="h4"
              textStyle="text-base"
              fontSize="16px"
              color={colors.foreground.secondary}
            >
              Your {currentRegionId} region is currently disabled. Would you
              like to enable it?
            </Text>
            <CreateEnvironmentWarning />
            <CreateEnvironmentButton
              regionId={currentRegionId}
              createRegion={createRegion}
              creatingRegionId={creatingRegionId}
              tenantIsBlocked={isOrganizationBlocked}
              size="lg"
              user={user}
            />
          </VStack>
        );
    }
  }
};

const EnableRegion = ({ user }: { user: User }) => {
  return (
    <VStack
      spacing={6}
      h="full"
      w="full"
      alignItems="center"
      justifyContent="center"
    >
      <EnableRegionContent user={user} />
    </VStack>
  );
};

export default EnableRegion;
