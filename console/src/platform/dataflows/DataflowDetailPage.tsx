// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Alert,
  AlertIcon,
  Button,
  HStack,
  Spinner,
  Text,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { useParams, useSearchParams } from "react-router-dom";

import { useDataflowGraphData } from "~/api/materialize/dataflow/useDataflowGraphData";
import { ErrorCode } from "~/api/materialize/types";
import ErrorBox from "~/components/ErrorBox";
import LabeledSelect from "~/components/LabeledSelect";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { useAllClusters } from "~/store/allClusters";

import {
  type CollapseState,
  defaultCollapseState,
  type VisibleNode,
} from "./dataflowGraph";
import { DataflowGraphView } from "./DataflowGraphView";
import { NodeDetailPanel } from "./NodeDetailPanel";

// Stable 32-bit hash so a pure stats refresh (identical node ids) keeps the
// same structure key and reuses the layout, while any structural change
// produces a new key and relayouts.
function hashString(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++)
    h = (Math.imul(h, 31) + s.charCodeAt(i)) | 0;
  return h;
}

const DataflowDetailPage = () => {
  const { clusterId, dataflowId } = useParams();
  const { getClusterById } = useAllClusters();
  const cluster = clusterId ? getClusterById(clusterId) : undefined;
  const [searchParams, setSearchParams] = useSearchParams();
  const replicaName = searchParams.get("replica") ?? cluster?.replicas[0]?.name;

  const params = React.useMemo(
    () =>
      cluster && replicaName && dataflowId
        ? { clusterName: cluster.name, replicaName, dataflowId }
        : undefined,
    [cluster, replicaName, dataflowId],
  );
  const { data, error, databaseError, loading, refetch } =
    useDataflowGraphData(params);

  const [collapsed, setCollapsed] = React.useState<CollapseState | null>(null);
  const [selectedNode, setSelectedNode] = React.useState<VisibleNode | null>(
    null,
  );
  // Digest of the sorted node ids: identical structure across a stats-only
  // refresh yields the same key, so layout and collapse state are preserved.
  const structureKey = data
    ? (() => {
        const ids = [...data.structure.nodes.keys()].sort();
        return `${params?.dataflowId}/${params?.replicaName}/${ids.length}-${hashString(ids.join(","))}`;
      })()
    : null;
  React.useEffect(() => {
    if (data) setCollapsed(defaultCollapseState(data.structure));
    // Reset collapse state when the structure identity changes.
  }, [structureKey]); // eslint-disable-line react-hooks/exhaustive-deps

  if (!cluster) return null;
  if (cluster.replicas.length === 0) {
    return (
      <MainContentContainer width="100%">
        <Text>This cluster has no replicas.</Text>
      </MainContentContainer>
    );
  }
  const permissionError =
    databaseError &&
    "code" in databaseError &&
    databaseError.code === ErrorCode.INSUFFICIENT_PRIVILEGE;

  return (
    <MainContentContainer width="100%">
      <VStack width="100%" height="100%" alignItems="stretch">
        <LabeledSelect
          label="Replica"
          value={replicaName ?? ""}
          onChange={(e) => setSearchParams({ replica: e.target.value })}
          flexShrink={0}
        >
          {cluster.replicas.map((r) => (
            <option key={r.name} value={r.name}>
              {r.name}
            </option>
          ))}
        </LabeledSelect>
        {data && (
          <HStack flexShrink={0}>
            <Button size="sm" onClick={refetch} isDisabled={loading}>
              Refresh
            </Button>
            <Text fontSize="sm" color="foreground.secondary">
              Last fetched {data.fetchedAt.toLocaleTimeString()}
            </Text>
          </HStack>
        )}
        {permissionError ? (
          <Alert status="info" rounded="md" p={4} width="auto">
            <AlertIcon />
            <Text>
              You&apos;ll need{" "}
              <Text as="span" textStyle="monospace">
                USAGE
              </Text>{" "}
              privilege on this cluster to visualize this dataflow.
            </Text>
          </Alert>
        ) : error ? (
          <ErrorBox message="There was an error visualizing your dataflow" />
        ) : !data || !collapsed ? (
          <Spinner />
        ) : data.structure.nodes.size <= 1 ? (
          <Text>This dataflow contains no operators.</Text>
        ) : (
          <HStack flex="1" minH={0} alignItems="stretch" spacing={0}>
            <DataflowGraphView
              structure={data.structure}
              collapsed={collapsed}
              onCollapsedChange={setCollapsed}
              cacheKey={structureKey ?? ""}
              onNodeClick={setSelectedNode}
              onPaneClick={() => setSelectedNode(null)}
            />
            {selectedNode && (
              <NodeDetailPanel
                node={selectedNode}
                onClose={() => setSelectedNode(null)}
              />
            )}
          </HStack>
        )}
      </VStack>
    </MainContentContainer>
  );
};

export default DataflowDetailPage;
