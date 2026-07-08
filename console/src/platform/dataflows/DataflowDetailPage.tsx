// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Alert, AlertIcon, Spinner, Text, VStack } from "@chakra-ui/react";
import React from "react";
import { useParams, useSearchParams } from "react-router-dom";

import { useDataflowGraphData } from "~/api/materialize/dataflow/useDataflowGraphData";
import { ErrorCode } from "~/api/materialize/types";
import ErrorBox from "~/components/ErrorBox";
import LabeledSelect from "~/components/LabeledSelect";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { useAllClusters } from "~/store/allClusters";

import { type CollapseState, defaultCollapseState } from "./dataflowGraph";
import { DataflowGraphView } from "./DataflowGraphView";

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
  const { data, error, databaseError } = useDataflowGraphData(params);

  const [collapsed, setCollapsed] = React.useState<CollapseState | null>(null);
  const structureKey = data
    ? `${params?.dataflowId}/${params?.replicaName}/${data.structure.nodes.size}`
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
          <DataflowGraphView
            structure={data.structure}
            collapsed={collapsed}
            onCollapsedChange={setCollapsed}
            cacheKey={structureKey ?? ""}
          />
        )}
      </VStack>
    </MainContentContainer>
  );
};

export default DataflowDetailPage;
