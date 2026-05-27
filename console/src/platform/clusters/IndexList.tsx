// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  FormLabel,
  HStack,
  MenuItem,
  Switch,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tooltip,
  Tr,
  useTheme,
} from "@chakra-ui/react";
import React from "react";
import { Link, useNavigate, useParams } from "react-router-dom";

import { useMaybeCurrentOrganizationId } from "~/api/auth";
import {
  createNamespace,
  isSystemCluster,
  isSystemId,
} from "~/api/materialize";
import { Replica } from "~/api/materialize/cluster/clusterList";
import { Index } from "~/api/materialize/cluster/indexesList";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import IndexListEmptyState from "~/components/IndexListEmptyState";
import { LoadingContainer } from "~/components/LoadingContainer";
import OverflowMenu from "~/components/OverflowMenu";
import DatabaseFilter from "~/components/SchemaObjectFilter/DatabaseFilter";
import SchemaFilter from "~/components/SchemaObjectFilter/SchemaFilter";
import {
  DatabaseFilterState,
  NameFilterState,
  SchemaFilterState,
  useSchemaObjectFilters,
} from "~/components/SchemaObjectFilter/useSchemaObjectFilters";
import SearchInput from "~/components/SearchInput";
import { useFlags } from "~/hooks/useFlags";
import useLocalStorage from "~/hooks/useLocalStorage";
import { InfoIcon } from "~/icons";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { useBuildIndexPath } from "~/platform/routeHelpers";
import { useAllClusters } from "~/store/allClusters";
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";
import { assert } from "~/util";
import { formatMemoryUsage } from "~/utils/format";

import { ClusterParams } from "./ClusterRoutes";
import { formatTableLagInfo } from "./format";
import {
  ArrangmentsMemoryUsageMap,
  LagMap,
  useArrangmentsMemory,
  useIndexesList,
  useMaterializationLag,
  useReplicasBySize,
} from "./queries";

const IndexList = () => {
  const { clusterId } = useParams<ClusterParams>();
  assert(clusterId);
  const maybeOrganizationId = useMaybeCurrentOrganizationId();
  const schemaObjectFilters = useSchemaObjectFilters("indexName");
  const { databaseFilter, schemaFilter, nameFilter } = schemaObjectFilters;
  const organizationIdKeyPrefix = maybeOrganizationId?.data
    ? `${maybeOrganizationId?.data ?? ""}-`
    : "";
  const [showSystemObjects, setShowSystemObjects] = useLocalStorage<boolean>(
    `${organizationIdKeyPrefix}${clusterId}-show-system-objects`,
    isSystemCluster(clusterId),
  );

  return (
    <MainContentContainer>
      <HStack mb="6" alignItems="center" justifyContent="space-between">
        <Text textStyle="heading-sm">Indexes</Text>
        <HStack>
          <FormLabel
            htmlFor="show-system-objects"
            variant="inline"
            textStyle="text-base"
          >
            Show introspection objects
          </FormLabel>
          <Switch
            id="show-system-objects"
            isChecked={showSystemObjects}
            onChange={() => setShowSystemObjects((value: boolean) => !value)}
          />
          <DatabaseFilter {...databaseFilter} />
          <SchemaFilter {...schemaFilter} />
          <SearchInput
            name="source"
            value={nameFilter.name}
            onChange={(e) => {
              nameFilter.setName(e.target.value);
            }}
          />
        </HStack>
      </HStack>
      <AppErrorBoundary message="An error has occurred loading indexes">
        <React.Suspense fallback={<LoadingContainer />}>
          <IndexListInner
            {...schemaObjectFilters}
            showSystemObjects={showSystemObjects}
          />
        </React.Suspense>
      </AppErrorBoundary>
    </MainContentContainer>
  );
};

interface IndexListInnerProps {
  databaseFilter: DatabaseFilterState;
  nameFilter: NameFilterState;
  schemaFilter: SchemaFilterState;
  showSystemObjects: boolean;
}

const IndexListInner = ({
  databaseFilter,
  schemaFilter,
  nameFilter,
  showSystemObjects,
}: IndexListInnerProps) => {
  const { clusterId } = useParams<ClusterParams>();
  assert(clusterId);
  const { getClusterById } = useAllClusters();
  const { data: indexes } = useIndexesList({
    clusterId,
    includeSystemObjects: showSystemObjects,
    databaseId: databaseFilter.selected?.id,
    schemaId: schemaFilter.selected?.id,
    nameFilter: nameFilter.name,
  });

  const cluster = getClusterById(clusterId);

  const arrangmentIds = React.useMemo(
    () => indexes?.rows.map((i) => i.id),
    [indexes],
  );

  const { data: sortedReplicas } = useReplicasBySize({
    clusterId,
    orderBy: "heapLimit",
  });
  const replicaName = sortedReplicas?.rows[0]?.name;
  const replicaHeapLimit = sortedReplicas?.rows[0]?.heapLimit;

  const { data } = useArrangmentsMemory({
    arrangmentIds,
    // bigint is not serializable. This conversion is lossy, but shouldn't matter in
    // practice.
    replicaHeapLimit: replicaHeapLimit ? Number(replicaHeapLimit) : undefined,
    replicaName,
    clusterName: cluster?.name,
  });
  const memoryUsageById = data?.memoryUsageById;

  const { data: materializationLagData } = useMaterializationLag({
    objectIds: arrangmentIds,
  });

  const { lagMap } = materializationLagData ?? {};

  const isEmpty = indexes && indexes.rows.length === 0;

  if (isEmpty) {
    return <IndexListEmptyState title="This cluster has no indexes" />;
  }
  return (
    <IndexTable
      indexes={indexes?.rows ?? []}
      replicas={cluster?.replicas ?? []}
      memoryUsageMap={memoryUsageById}
      lagMap={lagMap}
    />
  );
};

interface IndexTableProps {
  indexes: Index[];
  replicas: Replica[];
  memoryUsageMap: ArrangmentsMemoryUsageMap | undefined;
  lagMap?: LagMap;
}

const IndexTable = (props: IndexTableProps) => {
  const navigate = useNavigate();
  const flags = useFlags();
  const indexPath = useBuildIndexPath();
  const dataflowVisualizerEnabled = flags["visualization-features"];
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <>
      <Table variant="linkable" data-testid="index-table" borderRadius="xl">
        <Thead>
          <Tr>
            <Th>Name</Th>
            <Th>Object Name</Th>
            <Th>Type</Th>
            <Th>Memory Utilization</Th>
            <Th>Freshness</Th>
            {dataflowVisualizerEnabled && <Th></Th>}
          </Tr>
        </Thead>
        <Tbody>
          {props.indexes.map((i) => {
            const memoryStats = props.memoryUsageMap?.get(i.id);

            const lagInfo = props.lagMap?.get(i.id);

            const formattedLag = formatTableLagInfo(lagInfo);

            const rowIsClickable = !isSystemId(i.id);
            return (
              <Tr
                key={i.id}
                onClick={(e) => {
                  if (rowIsClickable) {
                    e.preventDefault();
                    navigate(indexPath(i));
                  }
                }}
                cursor={rowIsClickable ? "pointer" : "auto"}
              >
                <Td {...truncateMaxWidth} py="2">
                  <Text
                    textStyle="text-small"
                    fontWeight="500"
                    noOfLines={1}
                    color={colors.foreground.secondary}
                  >
                    {createNamespace(i.databaseName, i.schemaName)}
                  </Text>
                  <Text textStyle="text-ui-med" noOfLines={1}>
                    {i.name}
                  </Text>
                </Td>
                <Td py="2" {...truncateMaxWidth}>
                  <Text
                    textStyle="text-small"
                    fontWeight="500"
                    noOfLines={1}
                    mb="2px"
                    color={colors.foreground.secondary}
                  >
                    {createNamespace(i.databaseName, i.schemaName)}
                  </Text>
                  <Text textStyle="text-ui-reg" noOfLines={1}>
                    {i.relationName} ({i.indexedColumns.join(", ")})
                  </Text>
                </Td>
                <Td>{i.relationType}</Td>
                <Td data-testid="memory-usage">
                  {formatMemoryUsage(memoryStats)}{" "}
                  {lagInfo && !lagInfo.hydrated && (
                    <Tooltip label="Memory usage will continue increase until hydration is complete.">
                      <InfoIcon />
                    </Tooltip>
                  )}
                </Td>
                <Td>{formattedLag}</Td>
                {dataflowVisualizerEnabled && (
                  <Td width="16">
                    <OverflowMenu
                      items={[
                        {
                          visible: dataflowVisualizerEnabled,
                          render: () => (
                            <MenuItem
                              key="dataflow-visualizer"
                              as={Link}
                              to={`${indexPath(i)}/dataflow-visualizer`}
                              onClick={(e) => {
                                e.stopPropagation();
                              }}
                              textStyle="text-ui-med"
                            >
                              Visualize
                            </MenuItem>
                          ),
                        },
                      ]}
                    />
                  </Td>
                )}
              </Tr>
            );
          })}
        </Tbody>
      </Table>
    </>
  );
};

export default IndexList;
