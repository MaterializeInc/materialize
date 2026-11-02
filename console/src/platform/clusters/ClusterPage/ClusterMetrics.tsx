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
import { useParams } from "react-router-dom";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import ErrorBox from "~/components/ErrorBox";
import { LoadingContainer } from "~/components/LoadingContainer";
import TimePeriodSelect from "~/components/TimePeriodSelect";
import { useTimePeriodMinutes } from "~/hooks/useTimePeriodSelect";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { ClusterInfoBox } from "~/platform/clusters/ClusterOverview";
import { ClusterParams } from "~/platform/clusters/ClusterRoutes";
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "~/platform/clusters/constants";
import { useAllClusters } from "~/store/allClusters";
import { assert } from "~/util";

import { MemoryByObject } from "./MemoryByObject";
import { ReplicaSelect } from "./ReplicaSelect";
import { ResourceUsage } from "./ResourceUsage/ResourceUsage";
import { useSelectedReplica } from "./useSelectedReplica";

const ClusterMetricsContent = ({ cluster }: { cluster: Cluster }) => {
  const { selectedReplica, setSelectedReplicaName } =
    useSelectedReplica(cluster);
  const [timePeriodMinutes, setTimePeriodMinutes] = useTimePeriodMinutes({
    localStorageKey: "mz-cluster-graph-time-period",
  });

  return (
    <MainContentContainer mt="10">
      <VStack spacing="6">
        <HStack
          width="100%"
          justifyContent="space-between"
          flexWrap="wrap"
          spacing={4}
        >
          <ReplicaSelect
            replicas={cluster.replicas}
            value={selectedReplica?.name}
            onChange={setSelectedReplicaName}
            includeAll
          />
          <TimePeriodSelect
            timePeriodMinutes={timePeriodMinutes}
            setTimePeriodMinutes={setTimePeriodMinutes}
          />
        </HStack>
        <ClusterInfoBox cluster={cluster} />
        <ResourceUsage
          cluster={cluster}
          timePeriodMinutes={timePeriodMinutes}
          selectedReplicaId={selectedReplica?.id}
        />
        <MemoryByObject cluster={cluster} replica={selectedReplica} />
      </VStack>
    </MainContentContainer>
  );
};

export const ClusterMetrics = () => {
  const { clusterId } = useParams<ClusterParams>();
  assert(clusterId);
  const { getClusterById, isError } = useAllClusters();
  const cluster = getClusterById(clusterId);

  // `ClusterRoutes` redirects away from a cluster that doesn't exist, so a
  // missing one is still loading, unless the subscribe failed first.
  if (!cluster) {
    return isError ? (
      <ErrorBox message={CLUSTERS_FETCH_ERROR_MESSAGE} />
    ) : (
      <LoadingContainer />
    );
  }

  return <ClusterMetricsContent cluster={cluster} />;
};
