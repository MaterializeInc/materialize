// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Flex, Spinner, Text, useTheme } from "@chakra-ui/react";
import React from "react";

import Alert from "~/components/Alert";
import ErrorBox from "~/components/ErrorBox";
import LabeledSelect from "~/components/LabeledSelect";
import { WorkerSkewHeatmap } from "~/platform/clusters/ClusterOverview/WorkerSkewHeatmap";
import { useAllClusters } from "~/store/allClusters";
import { MaterializeTheme } from "~/theme";

import { pivotOperatorCpuPerWorker } from "./operatorSkewPivot";
import { MaintainedObjectListItem, useOperatorCpuPerWorker } from "./queries";

export interface ObjectPerformanceProps {
  item: MaintainedObjectListItem;
}

export const ObjectPerformance = ({ item }: ObjectPerformanceProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { getClusterById } = useAllClusters();
  const cluster = item.cluster ? getClusterById(item.cluster.id) : undefined;
  const replicas = cluster?.replicas;

  // Default to the first replica; user can switch via the selector.
  const [replicaName, setReplicaName] = React.useState<string | undefined>(
    () => replicas?.[0]?.name,
  );

  // If the cluster's replica set changes (new replica spun up, current one
  // dropped) and the selection is no longer valid, reset to the first one.
  React.useEffect(() => {
    if (!replicas || replicas.length === 0) {
      setReplicaName(undefined);
      return;
    }
    if (!replicaName || !replicas.some((r) => r.name === replicaName)) {
      setReplicaName(replicas[0].name);
    }
  }, [replicas, replicaName]);

  if (!item.cluster) {
    return (
      <Alert
        variant="info"
        showLabel={false}
        message="This object is not bound to a cluster, so no CPU breakdown is available."
      />
    );
  }

  if (!replicas || replicas.length === 0) {
    return (
      <Alert
        variant="info"
        showLabel={false}
        message={`${item.cluster.name} has no replicas. Increase its replication factor to see CPU distribution.`}
      />
    );
  }

  return (
    <Flex direction="column" gap={4}>
      <Flex justify="space-between" align="center" gap={4} wrap="wrap">
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          CPU breakdown of this object&apos;s dataflow operators across the
          workers on the selected replica, since the replica started.
        </Text>
        <LabeledSelect
          label="Replica"
          value={replicaName ?? ""}
          onChange={(e) => setReplicaName(e.target.value)}
        >
          {replicas.map((r) => (
            <option key={r.id} value={r.name}>
              {r.name}
              {r.size ? ` (${r.size})` : ""}
            </option>
          ))}
        </LabeledSelect>
      </Flex>
      {replicaName && (
        <OperatorHeatmap
          objectId={item.id}
          clusterName={item.cluster.name}
          replicaName={replicaName}
        />
      )}
    </Flex>
  );
};

interface OperatorHeatmapProps {
  objectId: string;
  clusterName: string;
  replicaName: string;
}

const OperatorHeatmap = ({
  objectId,
  clusterName,
  replicaName,
}: OperatorHeatmapProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data, isLoading, isError, error } = useOperatorCpuPerWorker({
    objectId,
    clusterName,
    replicaName,
  });

  const pivot = data ? pivotOperatorCpuPerWorker(data) : null;

  if (isLoading) {
    return (
      <Flex
        justifyContent="center"
        alignItems="center"
        py={16}
        color={colors.foreground.secondary}
      >
        <Spinner mr={3} />
        <Text>Loading operator CPU…</Text>
      </Flex>
    );
  }

  if (isError) {
    return (
      <ErrorBox
        message={
          error instanceof Error ? error.message : "Failed to load operator CPU"
        }
      />
    );
  }

  if (!pivot || pivot.rows.length === 0) {
    return (
      <Box py={12} textAlign="center" color={colors.foreground.secondary}>
        <Text>No operator activity recorded on this replica yet.</Text>
        <Text fontSize="xs" mt={1}>
          The dataflow may still be hydrating, or it has not produced enough
          scheduling samples since the replica started.
        </Text>
      </Box>
    );
  }

  return (
    <WorkerSkewHeatmap
      rows={pivot.rows}
      numWorkers={pivot.numWorkers}
      globalWorkerTotals={pivot.globalWorkerTotals}
      sortMode="skew"
      rowLabelHeader="Operator"
    />
  );
};

export default ObjectPerformance;
