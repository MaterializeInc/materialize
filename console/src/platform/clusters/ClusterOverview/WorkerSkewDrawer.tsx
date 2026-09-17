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
import { useAllObjectsLive } from "~/store/allObjectsCollection";
import { useRegionSlug } from "~/store/environments";
import { MaterializeTheme } from "~/theme";

import {
  CPU_MEASUREMENT_WINDOW_MS,
  useDataflowCpuMeasurement,
} from "../queries";
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

  const { state, rows, measuredAt, error, measure } = useDataflowCpuMeasurement(
    {
      clusterName,
      replicaName,
    },
  );

  // Names come from the objects collection rather than from the CPU query,
  // which is pinned to the replica and would plan a `mz_objects` join on the
  // customer's cluster. The collection seeds from its scoped cache before the
  // subscribe snapshot lands, so rows are named on a cold load too.
  const { data: allObjects } = useAllObjectsLive();
  const objectsById = React.useMemo(
    // Keyed once per change rather than scanned per row: the heatmap resolves a
    // name for every dataflow on the replica.
    () => new Map(allObjects.map((object) => [object.id, object])),
    [allObjects],
  );
  const resolveNaming = React.useCallback(
    (objectId: string) => {
      const object = objectsById.get(objectId);
      if (!object) return undefined;
      return {
        name: object.name,
        schemaName: object.schemaName,
        databaseName: object.databaseName,
      };
    },
    [objectsById],
  );

  const pivot = rows ? pivotDataflowCpuPerWorker(rows, resolveNaming) : null;

  if (state === "idle") {
    return (
      <Box py={12} textAlign="center" color={colors.foreground.secondary}>
        <Text>
          Measure CPU across workers over a {CPU_MEASUREMENT_WINDOW_MS / 1000}{" "}
          second window.
        </Text>
        <Text fontSize="xs" mt={1}>
          This runs two queries on {clusterName}, so it is not started
          automatically.
        </Text>
        <Button mt={4} onClick={measure}>
          Measure
        </Button>
      </Box>
    );
  }

  if (state === "sampling") {
    return (
      <Flex
        justifyContent="center"
        alignItems="center"
        py={16}
        color={colors.foreground.secondary}
      >
        <Spinner mr={3} />
        <Text>Sampling for {CPU_MEASUREMENT_WINDOW_MS / 1000} seconds…</Text>
      </Flex>
    );
  }

  if (state === "error") {
    return (
      <ErrorBox
        message={
          error instanceof Error ? error.message : "Failed to measure CPU"
        }
      />
    );
  }

  if (!pivot || pivot.rows.length === 0) {
    return (
      <Box py={12} textAlign="center" color={colors.foreground.secondary}>
        <Text>No dataflow used CPU during the sampling window.</Text>
        <Text fontSize="xs" mt={1}>
          Maintained objects show up here once they begin processing work.
        </Text>
        <Button mt={4} onClick={measure}>
          Measure again
        </Button>
      </Box>
    );
  }

  const handleRowClick = (row: DataflowRow) => {
    if (!row.objectId || !maintainedObjectsEnabled) return;
    navigate(`${regionPath(regionSlug)}/maintained-objects/${row.objectId}`);
  };

  return (
    <>
      <Flex
        justifyContent="space-between"
        alignItems="center"
        mb={3}
        color={colors.foreground.secondary}
      >
        <Text fontSize="xs">
          CPU over {CPU_MEASUREMENT_WINDOW_MS / 1000}s
          {measuredAt ? `, measured ${measuredAt.toLocaleTimeString()}` : null}
        </Text>
        <Button size="sm" variant="secondary" onClick={measure}>
          Measure again
        </Button>
      </Flex>
      <WorkerSkewHeatmap
        rows={pivot.rows}
        numWorkers={pivot.numWorkers}
        globalWorkerTotals={pivot.globalWorkerTotals}
        sortMode={sortMode}
        onRowClick={maintainedObjectsEnabled ? handleRowClick : undefined}
      />
    </>
  );
};

export default WorkerSkewDrawer;
