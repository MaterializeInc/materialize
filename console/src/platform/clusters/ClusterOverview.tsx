// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  Code,
  Flex,
  Grid,
  GridItem,
  HStack,
  ListItem,
  Spinner,
  Text,
  UnorderedList,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { useParams } from "react-router-dom";

import { MZ_PROBE_CLUSTER } from "~/api/materialize";
import { Cluster } from "~/api/materialize/cluster/clusterList";
import Alert from "~/components/Alert";
import ErrorBox from "~/components/ErrorBox";
import { EventEmitterProvider } from "~/components/EventEmitter";
import LabeledSelect from "~/components/LabeledSelect";
import TimePeriodSelect from "~/components/TimePeriodSelect";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import { useAppConfig } from "~/config/useAppConfig";
import { useFlags } from "~/hooks/useFlags";
import { useTimePeriodMinutes } from "~/hooks/useTimePeriodSelect";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { useAllClusters } from "~/store/allClusters";
import { MaterializeTheme } from "~/theme";
import { assert } from "~/util";

import ClusterFreshness from "./ClusterOverview/ClusterFreshness";
import { DataPoint } from "./ClusterOverview/types";
import {
  TOTAL_GRAPH_HEIGHT_PX,
  UtilizationGraph,
} from "./ClusterOverview/UtilizationGraph";
import { ClusterParams } from "./ClusterRoutes";
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "./constants";
import LargestMaintainedQueries from "./LargestMaintainedQueries";
import { useReplicaUtilizationHistory } from "./queries";

export interface ReplicaData {
  id: string;
  data: DataPoint[];
}

// because the data is sampled on 60s intervals, we don't want to show more granular data than this.
const MIN_BUCKET_SIZE_MS = 60 * 1000;

const GRAPH_SPACING = 24;

const ClusterOverview = () => {
  const { colors } = useTheme<MaterializeTheme>();
  const { clusterId } = useParams<ClusterParams>();

  assert(clusterId);
  const { getClusterById } = useAllClusters();
  const cluster = getClusterById(clusterId);
  const flags = useFlags();
  const [timePeriodMinutes, setTimePeriodMinutes] = useTimePeriodMinutes({
    localStorageKey: "mz-cluster-graph-time-period",
  });
  const [selectedReplica, setSelectedReplica] = React.useState("all");

  const bucketSizeMs = React.useMemo(
    () => Math.max(timePeriodMinutes * 1000, MIN_BUCKET_SIZE_MS),
    [timePeriodMinutes],
  );

  const replicaId = selectedReplica === "all" ? undefined : selectedReplica;

  const replicaUtilizationHistoryData = useReplicaUtilizationHistory({
    bucketSizeMs,
    timePeriodMinutes,
    clusterIds: [clusterId],
    replicaId,
  });

  const { data, isLoading, isError } = replicaUtilizationHistoryData;

  const { graphData } = data ?? {};

  const currentReplicas = React.useMemo(() => {
    return new Set(cluster?.replicas.map((r) => r.id));
  }, [cluster?.replicas]);

  const replicaColorMap = React.useMemo(() => {
    return new Map(
      (graphData ?? []).map((d, i) => {
        const replica = d.data[d.data.length - 1];

        const label = currentReplicas.has(replica.id)
          ? replica.name
          : replica.name + " (dropped)";
        return [
          d.id,
          {
            label,
            color: colors.lineGraph[i % colors.lineGraph.length],
          },
        ];
      }),
    );
  }, [colors.lineGraph, currentReplicas, graphData]);

  if (isError) {
    return <ErrorBox message={CLUSTERS_FETCH_ERROR_MESSAGE} />;
  }

  // It's possible to have a cluster where only some replicas have disk. Also, we still
  // want to show the graph for managed clusters that have disk, but no replicas.
  const clusterHasDisk =
    Boolean(cluster?.disk) || Boolean(cluster?.replicas.some((r) => r.disk));
  const graphContainerHeight = clusterHasDisk
    ? TOTAL_GRAPH_HEIGHT_PX * 2 + GRAPH_SPACING
    : TOTAL_GRAPH_HEIGHT_PX;

  return (
    <MainContentContainer mt="10">
      <VStack spacing="6">
        {cluster && <ClusterInfoBox cluster={cluster} />}
        <Box
          border={`solid 1px ${colors.border.primary}`}
          borderRadius="8px"
          py={4}
          px={6}
          width="100%"
          minW="460px"
        >
          <Flex
            width="100%"
            alignItems="start"
            justifyContent="space-between"
            mb="6"
          >
            <Text as="h3" fontSize="18px" lineHeight="20px" fontWeight={500}>
              Resource Usage
            </Text>
            <HStack>
              {cluster && (
                <LabeledSelect
                  label="Replicas"
                  value={selectedReplica}
                  onChange={(e) => setSelectedReplica(e.target.value)}
                >
                  <option value="all">All</option>
                  {cluster.replicas.map((r) => (
                    <option key={r.id} value={r.id}>
                      {r.name}
                    </option>
                  ))}
                </LabeledSelect>
              )}
              <TimePeriodSelect
                timePeriodMinutes={timePeriodMinutes}
                setTimePeriodMinutes={(timePeriod) => {
                  setTimePeriodMinutes(timePeriod);
                }}
              />
            </HStack>
          </Flex>
          {isLoading ? (
            <Flex
              height={graphContainerHeight}
              width="100%"
              alignItems="center"
              justifyContent="center"
            >
              <Spinner data-testid="loading-spinner" />
            </Flex>
          ) : data ? (
            <Grid
              gridTemplateColumns={{
                base: "minmax(100px, 1fr)",
                lg: "repeat(2, minmax(100px, 1fr))",
              }}
              gap={GRAPH_SPACING + "px"}
            >
              <EventEmitterProvider>
                <GridItem alignItems="flex-start" gap={6} width="100%">
                  <UtilizationGraph
                    bucketSizeMs={bucketSizeMs}
                    dataKey="cpuPercent"
                    data={data.graphData ?? []}
                    startTime={data.startDate}
                    endTime={data.endDate}
                    offlineEvents={data.offlineEvents}
                    replicaColorMap={replicaColorMap}
                    title="CPU"
                  />
                </GridItem>
                <GridItem>
                  <UtilizationGraph
                    bucketSizeMs={bucketSizeMs}
                    dataKey="heapPercent"
                    data={data.graphData ?? []}
                    startTime={data.startDate}
                    endTime={data.endDate}
                    offlineEvents={data.offlineEvents}
                    replicaColorMap={replicaColorMap}
                    title="Memory Utilization"
                  />
                </GridItem>
                {clusterHasDisk && (
                  // In self-managed, we don't show the disk graph
                  // because disk metrics are not available.
                  <AppConfigSwitch
                    cloudConfigElement={
                      <GridItem>
                        <UtilizationGraph
                          bucketSizeMs={bucketSizeMs}
                          dataKey="diskPercent"
                          data={data.graphData ?? []}
                          startTime={data.startDate}
                          endTime={data.endDate}
                          offlineEvents={data.offlineEvents}
                          replicaColorMap={replicaColorMap}
                          title="Disk"
                        />
                      </GridItem>
                    }
                  />
                )}
              </EventEmitterProvider>
            </Grid>
          ) : null}
        </Box>
        {cluster && (
          <Box width="100%">
            <LargestMaintainedQueries
              clusterId={cluster.id}
              clusterName={cluster.name}
            />
          </Box>
        )}
        {flags["console-freshness-2855"] && (
          <ClusterFreshness clusterId={clusterId} />
        )}
      </VStack>
    </MainContentContainer>
  );
};

const ClusterInfoBox = ({ cluster }: { cluster: Cluster }) => {
  const appConfig = useAppConfig();
  let message = null;

  const defaultSystemClusterInfoList = (
    <UnorderedList>
      <ListItem>
        You are <strong>not billed</strong> for this cluster.
      </ListItem>
      <ListItem>You cannot create objects in this cluster.</ListItem>
      <ListItem>You cannot alter or drop this cluster.</ListItem>
      <ListItem>
        You cannot run <code>SELECT</code> or <code>SUBSCRIBE</code> queries in
        this cluster.
      </ListItem>
    </UnorderedList>
  );

  if (cluster.name === "mz_catalog_server") {
    message = (
      <Text>
        This is a built-in system cluster that maintains several indexes to
        speed up <Code>SHOW</Code> commands and queries using the system
        catalog:
        <UnorderedList>
          <ListItem>
            You are <strong>not billed</strong> for this cluster.
          </ListItem>
          <ListItem>You cannot create objects in this cluster.</ListItem>
          <ListItem>You cannot alter or drop this cluster.</ListItem>
          <ListItem>
            You can run <code>SELECT</code> or <code>SUBSCRIBE</code> queries in
            this cluster as long as they only refer to objects in the system
            catalog.
          </ListItem>
        </UnorderedList>
      </Text>
    );
  } else if (cluster.name === MZ_PROBE_CLUSTER) {
    message = (
      <Text>
        This is a built-in system cluster used for internal uptime monitoring:
        {defaultSystemClusterInfoList}
      </Text>
    );
  } else if (cluster.name === "mz_support") {
    message = (
      <Text>
        This is a built-in system cluster used for internal support tasks:
        {defaultSystemClusterInfoList}
      </Text>
    );
  } else if (cluster.name === "mz_system") {
    message = (
      <Text>
        This is a built-in system cluster used for internal system jobs:
        {defaultSystemClusterInfoList}
      </Text>
    );
  } else if (cluster?.replicas.length === 0) {
    message = (
      <Text>
        This cluster is currently inactive. Increase its replication factor to
        resume processing.
      </Text>
    );
  } else if (
    cluster?.replicas.some((replica) =>
      replica.statuses.some(
        (status) =>
          status.status === "offline" &&
          new Date(status.updated_at) < new Date(Date.now() - 60 * 1000),
      ),
    ) &&
    appConfig.mode === "self-managed"
  ) {
    message = (
      <Text>
        One or more replicas have been offline for more than 1 minute. This
        usually indicates an issue deploying the pod or container. Please ensure
        there is capacity for the pod and that it is not being blocked by any
        admission webhooks.
      </Text>
    );
  }

  return (
    message && (
      <Alert
        variant="info"
        showLabel={true}
        message={message}
        width="100%"
        mb="5"
      />
    )
  );
};

export default ClusterOverview;
