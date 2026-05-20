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
  Button,
  ButtonGroup,
  Flex,
  Spinner,
  Tab,
  TabList,
  TabPanel,
  TabPanels,
  Tabs,
  Text,
  useTheme,
} from "@chakra-ui/react";
import React from "react";
import { useNavigate } from "react-router-dom";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import ErrorBox from "~/components/ErrorBox";
import { SideDrawer } from "~/components/SideDrawer";
import { useFlags } from "~/hooks/useFlags";
import { regionPath } from "~/platform/routeHelpers";
import { useRegionSlug } from "~/store/environments";
import { MaterializeTheme } from "~/theme";

import { useDataflowCpuPerWorker } from "../queries";
import { SortMode, WorkerSkewHeatmap } from "./WorkerSkewHeatmap";
import { DataflowRow, pivotDataflowCpuPerWorker } from "./workerSkewPivot";

const DrawerTitle = ({ clusterName }: { clusterName: string }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Flex direction="column" gap={0.5}>
      <Text textStyle="heading-sm">Where is CPU going?</Text>
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        {clusterName}
      </Text>
    </Flex>
  );
};

export interface WorkerSkewDrawerProps {
  isOpen: boolean;
  onClose: () => void;
  cluster: Cluster;
}

export const WorkerSkewDrawer = ({
  isOpen,
  onClose,
  cluster,
}: WorkerSkewDrawerProps) => {
  const [sortMode, setSortMode] = React.useState<SortMode>("skew");
  const replicas = cluster.replicas;

  return (
    <SideDrawer
      isOpen={isOpen}
      onClose={onClose}
      size="xl"
      width="min(1200px, 95vw)"
      title={<DrawerTitle clusterName={cluster.name} />}
      headerActions={
        <ButtonGroup size="sm" isAttached variant="outline">
          <Button
            isActive={sortMode === "skew"}
            onClick={() => setSortMode("skew")}
          >
            Sort by skew
          </Button>
          <Button
            isActive={sortMode === "cpu"}
            onClick={() => setSortMode("cpu")}
          >
            Sort by total CPU
          </Button>
        </ButtonGroup>
      }
    >
      {replicas.length === 0 ? (
        <Box p={6}>
          <Text>
            This cluster has no replicas. Increase its replication factor to see
            CPU distribution.
          </Text>
        </Box>
      ) : (
        <Tabs>
          <TabList px={4} pt={2}>
            {replicas.map((r) => (
              <Tab key={r.id}>
                {r.name}
                {r.size ? ` (${r.size})` : ""}
              </Tab>
            ))}
          </TabList>
          <TabPanels>
            {replicas.map((r) => (
              <TabPanel key={r.id} p={4}>
                <ReplicaHeatmap
                  clusterName={cluster.name}
                  replicaName={r.name}
                  sortMode={sortMode}
                />
              </TabPanel>
            ))}
          </TabPanels>
        </Tabs>
      )}
    </SideDrawer>
  );
};

interface ReplicaHeatmapProps {
  clusterName: string;
  replicaName: string;
  sortMode: SortMode;
}

const ReplicaHeatmap = ({
  clusterName,
  replicaName,
  sortMode,
}: ReplicaHeatmapProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const navigate = useNavigate();
  const regionSlug = useRegionSlug();
  const flags = useFlags();
  const maintainedObjectsEnabled = flags["maintained-objects-ui-50"];

  const { data, isLoading, isError, error } = useDataflowCpuPerWorker({
    clusterName,
    replicaName,
  });

  const pivot = data ? pivotDataflowCpuPerWorker(data) : null;

  if (isLoading) {
    return (
      <Flex
        justifyContent="center"
        alignItems="center"
        py={16}
        color={colors.foreground.secondary}
      >
        <Spinner mr={3} />
        <Text>Loading dataflow CPU…</Text>
      </Flex>
    );
  }

  if (isError) {
    return (
      <ErrorBox
        message={
          error instanceof Error ? error.message : "Failed to load dataflow CPU"
        }
      />
    );
  }

  if (!pivot || pivot.rows.length === 0) {
    return (
      <Box py={12} textAlign="center" color={colors.foreground.secondary}>
        <Text>No dataflows are scheduled on this replica yet.</Text>
        <Text fontSize="xs" mt={1}>
          Maintained objects show up here once they begin processing work.
        </Text>
      </Box>
    );
  }

  const handleRowClick = (row: DataflowRow) => {
    if (!row.objectId || !maintainedObjectsEnabled) return;
    navigate(`${regionPath(regionSlug)}/maintained-objects/${row.objectId}`);
  };

  return (
    <WorkerSkewHeatmap
      rows={pivot.rows}
      numWorkers={pivot.numWorkers}
      globalWorkerTotals={pivot.globalWorkerTotals}
      sortMode={sortMode}
      onRowClick={maintainedObjectsEnabled ? handleRowClick : undefined}
    />
  );
};

export default WorkerSkewDrawer;
