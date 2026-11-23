// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Flex, Spinner, Text, useTheme, VStack } from "@chakra-ui/react";
import { sort } from "d3";
import React from "react";
import { useParams } from "react-router-dom";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import ErrorBox from "~/components/ErrorBox";
import { LoadingContainer } from "~/components/LoadingContainer";
import TextLink from "~/components/TextLink";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { ClusterParams } from "~/platform/clusters/ClusterRoutes";
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "~/platform/clusters/constants";
import { useUnhydratedObjectCounts } from "~/platform/clusters/queries";
import { useAllClusters } from "~/store/allClusters";
import { MaterializeTheme } from "~/theme";
import { assert } from "~/util";

import { ReplicaSelect } from "./ReplicaSelect";
import { currentReplicaColor } from "./ResourceUsage/resourceUsageModel";
import { processesTabLink } from "./tabLinks";
import { useSelectedReplica } from "./useSelectedReplica";
import { useSizedReplicas } from "./useSizedReplicas";
import { CPU_DOCS, WorkerReplica } from "./WorkerCpu/workerCpuModel";
import { WorkerCpuPanel } from "./WorkerCpu/WorkerCpuPanel";

const Message = ({ children }: React.PropsWithChildren) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Text textStyle="text-base" color={colors.foreground.secondary}>
      {children}
    </Text>
  );
};

const ClusterWorkersContent = ({ cluster }: { cluster: Cluster }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { selectedReplicaName, selectedReplica, setSelectedReplicaName } =
    useSelectedReplica(cluster);
  const sized = useSizedReplicas(cluster);
  const workerReplicas = React.useMemo(() => {
    const colorOf = currentReplicaColor(cluster.replicas, colors.lineGraph);
    return sort(sized.replicas, ({ name }) => name).map(
      (replica): WorkerReplica => ({
        ...replica,
        color: colorOf(replica.id),
      }),
    );
  }, [cluster.replicas, sized.replicas, colors.lineGraph]);
  // A link without a replica shows the first, and leaves the URL as it is, so
  // the other tabs keep showing every replica.
  const shownName = selectedReplicaName ?? workerReplicas.at(0)?.name;
  const replica = workerReplicas.find(({ name }) => name === shownName);
  const unhydratedObjectCounts = useUnhydratedObjectCounts(
    cluster.replicas.map(({ id }) => id).sort(),
  );

  let body: React.ReactNode;
  if (replica) {
    body = (
      <WorkerCpuPanel
        key={replica.id}
        clusterId={cluster.id}
        clusterName={cluster.name}
        replica={replica}
        unhydratedCount={unhydratedObjectCounts.data?.get(replica.id)}
        processesLink={
          replica.allocation.processes > 1
            ? processesTabLink(replica.name)
            : undefined
        }
      />
    );
  } else if (sized.isError) {
    body = <ErrorBox message="Replica sizes are unavailable." />;
  } else if (sized.isLoading) {
    body = (
      <Flex justifyContent="center" py={10}>
        <Spinner />
      </Flex>
    );
  } else if (cluster.replicas.length === 0) {
    body = <Message>{cluster.name} has no replicas.</Message>;
  } else if (selectedReplicaName !== undefined && !selectedReplica) {
    body = (
      <Message>
        {cluster.name} has no replica {selectedReplicaName}.
      </Message>
    );
  } else {
    body = (
      <Message>
        {selectedReplica
          ? `The worker count for ${selectedReplica.name}'s size is unavailable.`
          : `Worker counts for ${cluster.name}'s replica sizes are unavailable.`}
      </Message>
    );
  }

  return (
    <MainContentContainer mt="10">
      <VStack spacing="6" alignItems="stretch">
        {workerReplicas.length > 0 && (
          <ReplicaSelect
            replicas={workerReplicas}
            value={replica?.name}
            onChange={setSelectedReplicaName}
            placeholder="Choose a replica"
          />
        )}
        <Text textStyle="text-base" color={colors.foreground.secondary}>
          Each worker is a thread that runs a share of every object&apos;s work.
          Work is split between workers by key, so when most rows share one key
          value, one worker does most of the work while the others wait.{" "}
          <TextLink href={`${CPU_DOCS}#worker-skew`} isExternal>
            Learn more
          </TextLink>
        </Text>
        {body}
      </VStack>
    </MainContentContainer>
  );
};

/** How each replica's workers share its CPU. */
export const ClusterWorkers = () => {
  const { clusterId } = useParams<ClusterParams>();
  assert(clusterId);
  const { getClusterById, isError } = useAllClusters();
  const cluster = getClusterById(clusterId);

  // As on the Metrics tab, a missing cluster is still loading.
  if (!cluster) {
    return isError ? (
      <ErrorBox message={CLUSTERS_FETCH_ERROR_MESSAGE} />
    ) : (
      <LoadingContainer />
    );
  }

  return <ClusterWorkersContent cluster={cluster} />;
};
