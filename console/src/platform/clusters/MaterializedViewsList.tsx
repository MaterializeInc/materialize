// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Flex,
  HStack,
  MenuItem,
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

import { createNamespace } from "~/api/materialize";
import { Replica } from "~/api/materialize/cluster/clusterList";
import {
  MaterializedView,
  MaterializedViewsResponse,
  useMaterializedViews,
} from "~/api/materialize/cluster/useMaterializedViews";
import { Alert } from "~/components/Alert";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { CodeBlock } from "~/components/copyableComponents";
import { LoadingContainer } from "~/components/LoadingContainer";
import OverflowMenu from "~/components/OverflowMenu";
import DatabaseFilter from "~/components/SchemaObjectFilter/DatabaseFilter";
import SchemaFilter from "~/components/SchemaObjectFilter/SchemaFilter";
import { useSchemaObjectFilters } from "~/components/SchemaObjectFilter/useSchemaObjectFilters";
import SearchInput from "~/components/SearchInput";
import { useFlags } from "~/hooks/useFlags";
import { ClustersIcon, InfoIcon } from "~/icons";
import { MainContentContainer } from "~/layouts/BaseLayout";
import {
  EmptyListHeader,
  EmptyListHeaderContents,
  EmptyListWrapper,
  IconBox,
  SampleCodeBoxWrapper,
} from "~/layouts/listPageComponents";
import docUrls from "~/mz-doc-urls.json";
import { useBuildMaterializedViewPath } from "~/platform/routeHelpers";
import { useAllClusters } from "~/store/allClusters";
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";
import { assert } from "~/util";
import { formatMemoryUsage } from "~/utils/format";

import { ClusterParams } from "./ClusterRoutes";
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "./constants";
import { formatTableLagInfo } from "./format";
import {
  ArrangmentsMemoryUsageMap,
  LagMap,
  useArrangmentsMemory,
  useMaterializationLag,
  useReplicasBySize,
} from "./queries";

const createExample = `CREATE MATERIALIZED VIEW winning_bids AS
  SELECT auction_id,
         bid_id,
         item,
         amount
  FROM highest_bid_per_auction
  WHERE end_time < mz_now();`;

const MaterializedViews = () => {
  const { clusterId } = useParams<ClusterParams>();
  const { databaseFilter, schemaFilter, nameFilter } = useSchemaObjectFilters(
    "materializeViewName",
  );

  const materializedViewsResponse = useMaterializedViews({
    databaseId: databaseFilter.selected?.id,
    schemaId: schemaFilter.selected?.id,
    nameFilter: nameFilter.name,
    clusterId,
  });
  const { isInitiallyLoading, failedToLoad } = materializedViewsResponse;

  return (
    <MainContentContainer>
      <HStack mb="6" alignItems="center" justifyContent="space-between">
        <Text textStyle="heading-sm">Materialized Views</Text>
        <HStack>
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
      {isInitiallyLoading ? (
        <LoadingContainer />
      ) : failedToLoad ? (
        <Flex
          width="100%"
          height="100%"
          alignItems="center"
          justifyContent="center"
        >
          <Alert variant="error" message={CLUSTERS_FETCH_ERROR_MESSAGE} />
        </Flex>
      ) : (
        <AppErrorBoundary message="An error has occurred loading indexes">
          <React.Suspense fallback={<LoadingContainer />}>
            <MaterializedViewListInner
              materializedViewsResponse={materializedViewsResponse}
            />
          </React.Suspense>
        </AppErrorBoundary>
      )}
    </MainContentContainer>
  );
};

const MaterializedViewListInner = ({
  materializedViewsResponse,
}: {
  materializedViewsResponse: MaterializedViewsResponse;
}) => {
  const { clusterId } = useParams<ClusterParams>();
  assert(clusterId);
  const { getClusterById } = useAllClusters();
  const cluster = getClusterById(clusterId);
  const { data: materializedViews } = materializedViewsResponse;

  const arrangmentIds = React.useMemo(
    () => materializedViews?.map((mv) => mv.id),
    [materializedViews],
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

  const lagMap = materializationLagData?.lagMap;

  const isEmpty = materializedViews && materializedViews.length === 0;

  if (isEmpty) {
    return (
      <EmptyListWrapper>
        <EmptyListHeader>
          <IconBox type="Missing">
            <ClustersIcon />
          </IconBox>
          <EmptyListHeaderContents
            title="This cluster has no materialized views"
            helpText="Materialized views are one of the most powerful features of materalize."
          />
        </EmptyListHeader>
        <SampleCodeBoxWrapper
          docsUrl={docUrls["/docs/sql/create-materialized-view/"]}
        >
          <CodeBlock
            lineNumbers
            title="Create a materialized view"
            contents={createExample}
          >
            {createExample}
          </CodeBlock>
        </SampleCodeBoxWrapper>
      </EmptyListWrapper>
    );
  }
  return (
    <AppErrorBoundary message="An error has occurred loading indexes">
      <React.Suspense fallback={<LoadingContainer />}>
        <MaterializedViewTable
          materializedViews={materializedViews ?? []}
          replicas={cluster?.replicas ?? []}
          memoryUsageMap={memoryUsageById}
          lagMap={lagMap}
        />
      </React.Suspense>
    </AppErrorBoundary>
  );
};

interface MaterializedViewTableProps {
  materializedViews: MaterializedView[];
  replicas: Replica[];
  memoryUsageMap: ArrangmentsMemoryUsageMap | undefined;
  lagMap?: LagMap;
}

const MaterializedViewTable = (props: MaterializedViewTableProps) => {
  const navigate = useNavigate();
  const flags = useFlags();
  const materializedViewPath = useBuildMaterializedViewPath();
  const dataflowVisualizerEnabled = flags["visualization-features"];

  const { colors } = useTheme<MaterializeTheme>();

  return (
    <>
      <Table
        variant="linkable"
        data-testid="materialized-view-table"
        borderRadius="xl"
      >
        <Thead>
          <Tr>
            <Th>Name</Th>
            <Th>Memory Utilization</Th>
            <Th>Freshness</Th>
            {dataflowVisualizerEnabled && <Th></Th>}
          </Tr>
        </Thead>
        <Tbody>
          {props.materializedViews.map((v) => {
            const memoryStats = props.memoryUsageMap?.get(v.id);

            const lagInfo = props.lagMap?.get(v.id);

            const formattedLag = formatTableLagInfo(lagInfo);

            return (
              <Tr
                key={v.name}
                onClick={() => {
                  navigate(materializedViewPath(v));
                }}
                cursor="pointer"
              >
                <Td {...truncateMaxWidth} py="2">
                  <Text
                    textStyle="text-small"
                    fontWeight="500"
                    noOfLines={1}
                    color={colors.foreground.secondary}
                  >
                    {createNamespace(v.databaseName, v.schemaName)}
                  </Text>
                  <Text textStyle="text-ui-med" noOfLines={1}>
                    {v.name}
                  </Text>
                </Td>
                <Td data-testid="memory-usage">
                  {formatMemoryUsage(memoryStats)}
                  {lagInfo && !lagInfo.hydrated && (
                    <Tooltip label="Memory usage will continue increase until hydration is complete.">
                      <InfoIcon ml="1" />
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
                              to={`${materializedViewPath(v)}/dataflow-visualizer`}
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

export default MaterializedViews;
