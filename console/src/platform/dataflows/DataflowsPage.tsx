// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Spinner,
  Table,
  Tbody,
  Td,
  Th,
  Thead,
  Tr,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { Link, Navigate, useParams, useSearchParams } from "react-router-dom";

import { useDataflowIdForExport } from "~/api/materialize/dataflow/useDataflowIdForExport";
import { useDataflowList } from "~/api/materialize/dataflow/useDataflowList";
import { isInsufficientPrivilegeError } from "~/api/materialize/executeSql";
import ErrorBox from "~/components/ErrorBox";
import LabeledSelect from "~/components/LabeledSelect";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { replicaSearch } from "~/platform/routeHelpers";
import { useAllClusters } from "~/store/allClusters";
import { formatBytesShort, formatElapsedNs } from "~/utils/format";

import { formatCount } from "./nodeStyle";
import { UsagePrivilegeAlert } from "./UsagePrivilegeAlert";

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
    databaseError: exportDatabaseError,
  } = useDataflowIdForExport(exportParams);

  if (!cluster) return null;

  if (exportParams) {
    if (exportLoading) return <Spinner />;
    if (dataflowId !== null) {
      return (
        <Navigate to={`${dataflowId}${replicaSearch(replicaName)}`} replace />
      );
    }
  }
  const permissionError =
    isInsufficientPrivilegeError(databaseError) ||
    isInsufficientPrivilegeError(exportDatabaseError);

  // Either way the list below still renders, so a failed deep-link leaves
  // the user somewhere useful. The two cases read differently though: no
  // running dataflow is an ordinary answer about this replica, while a
  // failed lookup means the question wasn't answered at all and must not be
  // reported as an absence.
  const exportResolved = exportParams !== undefined && !exportLoading;
  const exportLookupFailed = exportResolved && !!exportError;
  const exportHasNoDataflow =
    exportResolved && !exportError && dataflowId === null;
  return (
    <MainContentContainer width="100%">
      <VStack alignItems="stretch">
        {exportLookupFailed && (
          <ErrorBox message="There was an error finding this object's dataflow" />
        )}
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
          <UsagePrivilegeAlert action="list its dataflows" />
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
                <Th isNumeric>Elapsed</Th>
              </Tr>
            </Thead>
            <Tbody>
              {(data ?? []).map((d) => (
                <Tr key={d.id}>
                  <Td>
                    <Link to={`${d.id}${replicaSearch(replicaName)}`}>
                      {d.name}
                    </Link>
                  </Td>
                  <Td isNumeric>{formatCount(d.records)}</Td>
                  <Td isNumeric>{formatBytesShort(d.size)}</Td>
                  <Td isNumeric>{formatElapsedNs(d.elapsedNs)}</Td>
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
