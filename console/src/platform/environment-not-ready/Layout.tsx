// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Flex, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import { useAtomValue } from "jotai";
import React from "react";
import { Link } from "react-router-dom";

import { MaterializeLogo } from "~/components/MaterializeLogo";
import TextLink from "~/components/TextLink";
import { User } from "~/external-library-wrappers/frontegg";
import { useToast } from "~/hooks/useToast";
import EnvironmentSelect from "~/layouts/EnvironmentSelect";
import PageFooter from "~/layouts/PageFooter";
import ProfileDropdown from "~/layouts/ProfileDropdown";
import { NAVBAR_Z_INDEX } from "~/layouts/zIndex";
import { regionPath } from "~/platform/routeHelpers";
import {
  currentRegionIdAtom,
  useEnvironmentsWithHealth,
  useRegionSlug,
} from "~/store/environments";
import { MaterializeTheme } from "~/theme";

const REGION_READY_TOAST_ID = "region-ready-toast";

export const NAVBAR_HEIGHT = "16";

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

export const EnvironmentNotReadyStatus = ({ user }: { user: User }) => {
  const toast = useToast();
  const regionSlug = useRegionSlug();
  const environments = useEnvironmentsWithHealth();
  const currentRegionId = useAtomValue(currentRegionIdAtom);
  const currentEnvironment = environments.get(currentRegionId);
  const currentRegionReady =
    currentEnvironment &&
    "status" in currentEnvironment &&
    currentEnvironment.status.health === "healthy";

  const anyOtherRegionReady = React.useMemo(
    () =>
      Array.from(environments.entries()).some(([regionId, env]) => {
        if (regionId === currentRegionId) return false;
        return (
          env &&
          env.state === "enabled" &&
          (env.status.health === "healthy" || env.status.health === "blocked")
        );
      }),
    [currentRegionId, environments],
  );

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
      // Close to toast when this component unmounts
      toast.close(REGION_READY_TOAST_ID);
    };
    // The toast reference isn't stable, including it in the dependency array leads to
    // edge cases where the toast never shows up.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  if (!currentEnvironment) return;

  return (
    <HStack spacing="6">
      <HStack as={Link} to="/" spacing="2" flexShrink="0">
        <MaterializeLogo markOnly height="6" width="6" />
        <Text
          textStyle="heading-lg"
          fontSize="md"
          display={{ sm: "none", md: "inline" }}
        >
          Materialize
        </Text>
      </HStack>
      {!currentRegionReady && anyOtherRegionReady && (
        <EnvironmentSelect user={user} />
      )}
    </HStack>
  );
};

export const EnvironmentNotReadyLayout = (props: {
  children: React.ReactNode;
  user: User;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <VStack
      height="100vh"
      minW="100vw"
      overflowX="hidden"
      data-testid="page-layout"
    >
      <Flex
        alignItems="center"
        background={colors.background.primary}
        justifyContent="space-between"
        px="16"
        py="2"
        height={NAVBAR_HEIGHT}
        position={{ base: "initial", xl: "fixed" }}
        flexDirection={{ base: "column", sm: "row" }}
        top="0"
        right="0"
        left="0"
        zIndex={NAVBAR_Z_INDEX}
        width="100%"
      >
        <EnvironmentNotReadyStatus user={props.user} />
        <ProfileDropdown
          isCollapsed={false}
          border="none"
          borderRadius="40px"
        />
      </Flex>
      <Flex
        flex="1"
        minHeight="0"
        alignItems="center"
        width="100%"
        overflowY="auto"
      >
        {props.children}
      </Flex>
      <PageFooter width="100%" />
    </VStack>
  );
};
