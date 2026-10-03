// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, VStack } from "@chakra-ui/react";
import React from "react";
import { useParams } from "react-router-dom";

import { LoadingContainer } from "~/components/LoadingContainer";
import { useFlags } from "~/hooks/useFlags";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { ClusterInfoBox } from "~/platform/clusters/ClusterOverview";
import ClusterFreshness from "~/platform/clusters/ClusterOverview/ClusterFreshness";
import { ClusterParams } from "~/platform/clusters/ClusterRoutes";
import LargestMaintainedQueries from "~/platform/clusters/LargestMaintainedQueries";
import { useAllClusters } from "~/store/allClusters";
import { assert } from "~/util";

import { ResourceUsage } from "./ResourceUsage/ResourceUsage";

export const ClusterMetrics = () => {
  const { clusterId } = useParams<ClusterParams>();
  assert(clusterId);
  const { getClusterById } = useAllClusters();
  const cluster = getClusterById(clusterId);
  const flags = useFlags();

  // `ClusterRoutes` redirects away from a cluster that doesn't exist, so a
  // missing one is still loading.
  if (!cluster) return <LoadingContainer />;

  return (
    <MainContentContainer mt="10">
      <VStack spacing="6">
        <ClusterInfoBox cluster={cluster} />
        <ResourceUsage cluster={cluster} />
        <Box width="100%">
          <LargestMaintainedQueries
            clusterId={cluster.id}
            clusterName={cluster.name}
          />
        </Box>
        {flags["console-freshness-2855"] && (
          <ClusterFreshness clusterId={clusterId} />
        )}
      </VStack>
    </MainContentContainer>
  );
};
