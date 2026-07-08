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
  Spinner,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { Link, Navigate, useParams, useSearchParams } from "react-router-dom";

import { useDataflowIdForExport } from "~/api/materialize/dataflow/useDataflowIdForExport";
import { useDataflowList } from "~/api/materialize/dataflow/useDataflowList";
import { ErrorCode } from "~/api/materialize/types";
import ErrorBox from "~/components/ErrorBox";
import LabeledSelect from "~/components/LabeledSelect";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { useAllClusters } from "~/store/allClusters";
import { formatBytesShort } from "~/utils/format";

const DataflowsPage = () => {
  const { clusterId } = useParams();
  const { getClusterById } = useAllClusters();
  const cluster = clusterId ? getClusterById(clusterId) : undefined;
  const [searchParams, setSearchParams] = useSearchParams();
  const replicaName = searchParams.get("replica") ?? cluster?.replicas[0]?.name;
  const params = React.useMemo(
    () =>
      cluster && replicaName
        ? { clusterName: cluster.name, replicaName }
        : undefined,
    [cluster, replicaName],
  );
  const { data, error, databaseError, loading } = useDataflowList(params);

  // When an object is deep-linked via ?export=<id>, resolve its running
  // dataflow on the chosen replica and redirect to that dataflow's page. The
  // hook is called unconditionally and no-ops when there is no export to
  // resolve.
  const exportId = searchParams.get("export") ?? undefined;
  const exportParams = React.useMemo(
    () =>
      cluster && replicaName && exportId
        ? { clusterName: cluster.name, replicaName, exportId }
        : undefined,
    [cluster, replicaName, exportId],
  );
  const {
    dataflowId,
    loading: exportLoading,
    error: exportError,
  } = useDataflowIdForExport(exportParams);

  if (!cluster) return null;

  if (exportParams) {
    if (exportLoading) return <Spinner />;
    if (dataflowId !== null) {
      return <Navigate to={`${dataflowId}?replica=${replicaName}`} replace />;
    }
  }
  const permissionError =
    databaseError &&
    "code" in databaseError &&
    databaseError.code === ErrorCode.INSUFFICIENT_PRIVILEGE;

  // The export resolved cleanly but no dataflow is running for it on this
  // replica. Surface that above the list, which still renders below.
  const exportHasNoDataflow =
    exportParams !== undefined &&
    !exportLoading &&
    !exportError &&
    dataflowId === null;
  return (
    <MainContentContainer width="100%">
      <VStack alignItems="stretch">
        {exportHasNoDataflow && (
          <ErrorBox message="This object has no running dataflow on the selected replica." />
        )}
        <LabeledSelect
          label="Replica"
          value={replicaName ?? ""}
          onChange={(e) => setSearchParams({ replica: e.target.value })}
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
              privilege on this cluster to list its dataflows.
            </Text>
          </Alert>
        ) : error ? (
          <ErrorBox message="There was an error listing dataflows" />
        ) : !data && loading ? (
          <Spinner />
        ) : (
          <Table size="sm">
            <Thead>
              <Tr>
                <Th>Name</Th>
                <Th isNumeric>Records</Th>
                <Th isNumeric>Size</Th>
                <Th isNumeric>Scheduled</Th>
              </Tr>
            </Thead>
            <Tbody>
              {(data ?? []).map((d) => (
                <Tr key={d.id}>
                  <Td>
                    <Link to={`${d.id}?replica=${replicaName}`}>{d.name}</Link>
                  </Td>
                  <Td isNumeric>{d.records.toString()}</Td>
                  <Td isNumeric>{formatBytesShort(d.size)}</Td>
                  <Td isNumeric>{Math.round(Number(d.elapsedNs) / 1e9)}s</Td>
                </Tr>
              ))}
            </Tbody>
          </Table>
        )}
      </VStack>
    </MainContentContainer>
  );
};

export default DataflowsPage;
