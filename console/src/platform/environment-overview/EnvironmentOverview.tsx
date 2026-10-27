// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, VStack } from "@chakra-ui/react";
import React from "react";

import { useFlags } from "~/hooks/useFlags";
import {
  MainContentContainer,
  PageHeader,
  PageHeading,
} from "~/layouts/BaseLayout";

import ClusterFreshness from "./ClusterFreshness";
import MemDiskUtilization from "./MemDiskUtilization";

export const EnvironmentOverview = () => {
  const flags = useFlags();
  return (
    <MainContentContainer>
      <PageHeader boxProps={{ mb: 4 }}>
        <HStack>
          <PageHeading>Environment Overview</PageHeading>
        </HStack>
      </PageHeader>
      <VStack alignItems="flex-start" width="100%" gap="10" paddingBottom="10">
        <MemDiskUtilization />
        {flags["console-freshness-2855"] && <ClusterFreshness />}
      </VStack>
    </MainContentContainer>
  );
};

export default EnvironmentOverview;
