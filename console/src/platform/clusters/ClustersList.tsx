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
  Switch,
  Text,
  Tooltip,
  VStack,
} from "@chakra-ui/react";
import { createColumnHelper } from "@tanstack/react-table";
import React from "react";
import { Link as RouterLink } from "react-router-dom";

import { isSystemCluster } from "~/api/materialize";
import { ClusterWithOwnership } from "~/api/materialize/cluster/clusterList";
import useLatestOfflineReplica, {
  LatestOfflineReplicaMap,
} from "~/api/materialize/cluster/useLatestOfflineReplica";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { CodeBlock } from "~/components/copyableComponents";
import DeleteObjectMenuItem from "~/components/DeleteObjectMenuItem";
import { LoadingContainer } from "~/components/LoadingContainer";
import OverflowMenu, { OVERFLOW_BUTTON_WIDTH } from "~/components/OverflowMenu";
import { sortingFunctions } from "~/components/Table/tableColumnBuilders";
import { TablePagination } from "~/components/Table/TablePagination";
import { TableSearch } from "~/components/Table/TableSearch";
import { UniversalTable } from "~/components/Table/UniversalTable";
import { useUniversalTable } from "~/components/Table/useUniversalTable";
import TextLink from "~/components/TextLink";
import { ClustersIcon, InfoIcon } from "~/icons";
import {
  MainContentContainer,
  PageHeader,
  PageHeading,
} from "~/layouts/BaseLayout";
import {
  EmptyListHeader,
  EmptyListHeaderContents,
  EmptyListWrapper,
  IconBox,
  SampleCodeBoxWrapper,
} from "~/layouts/listPageComponents";
import docUrls from "~/mz-doc-urls.json";
import { relativeClusterPath } from "~/platform/routeHelpers";
import WarningIcon from "~/svg/WarningIcon";
import { truncateMaxWidth } from "~/theme/components/Table";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import AlterClusterMenuItem from "./AlterClusterMenuItem";
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "./constants";
import { useClusters } from "./queries";
import { useShowSystemObjects } from "./useShowSystemObjects";

const createClusterSuggestion = {
  title: "Create a cluster",
  string: `CREATE CLUSTER <cluster_name>
  (SIZE = '25cc');`,
};

/**
 * Shared data threaded into cell components via TanStack's table `meta`.
 * Read from `info.table.options.meta` and cast to this shape inside cells.
 */
interface ClusterTableMeta {
  refetchClusters: () => void;
  offlineReplicaMap: LatestOfflineReplicaMap | undefined;
}

const ClusterNameCell = ({ cluster }: { cluster: ClusterWithOwnership }) => (
  <HStack>
    <TextLink
      as={RouterLink}
      to={relativeClusterPath(cluster)}
      textStyle="text-ui-med"
      noOfLines={1}
    >
      {cluster.name}
    </TextLink>
    {isSystemCluster(cluster.id) && (
      <Tooltip
        label="This is a built-in system cluster. You are not billed for this cluster."
        lineHeight={1.2}
      >
        <InfoIcon />
      </Tooltip>
    )}
  </HStack>
);

const LastStatusChangeCell = ({
  cluster,
  offlineReplicaMap,
}: {
  cluster: ClusterWithOwnership;
  offlineReplicaMap: LatestOfflineReplicaMap | undefined;
}) => {
  const offlineStatus = offlineReplicaMap?.get(cluster.id);

  const lastStatusChangeString = cluster.latestStatusUpdate
    ? formatDate(
        cluster.latestStatusUpdate,
        FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
      )
    : "-";

  return (
    <HStack>
      <Text noOfLines={1} paddingRight="6" position="relative">
        {lastStatusChangeString}
        {offlineStatus?.shouldSurfaceOom && (
          <Tooltip
            px={3}
            py={2}
            minWidth="fit-content"
            rounded="md"
            label={`A replica ran out of memory on ${formatDate(
              offlineStatus.lastOfflineAt,
              FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
            )}`}
          >
            <WarningIcon position="absolute" right="0" />
          </Tooltip>
        )}
      </Text>
    </HStack>
  );
};

const ClusterActionsCell = ({
  cluster,
  refetchClusters,
}: {
  cluster: ClusterWithOwnership;
  refetchClusters: () => void;
}) => (
  <OverflowMenu
    items={[
      {
        visible: !isSystemCluster(cluster.id) && cluster.isOwner,
        render: () => (
          <>
            {cluster.managed && <AlterClusterMenuItem cluster={cluster} />}
            <DeleteObjectMenuItem
              key="delete-object"
              selectedObject={cluster}
              onSuccessAction={refetchClusters}
              objectType="CLUSTER"
            />
          </>
        ),
      },
    ]}
  />
);

const columnHelper = createColumnHelper<ClusterWithOwnership>();

const columns = [
  columnHelper.accessor("name", {
    header: "Name",
    sortingFn: "alphanumeric",
    cell: (info) => <ClusterNameCell cluster={info.row.original} />,
    meta: {
      minWidth: { md: "280px", sm: "auto" },
      cellProps: truncateMaxWidth,
    },
  }),
  columnHelper.accessor((row) => row.replicas.length, {
    id: "replicaCount",
    header: "Replicas",
    sortingFn: "basic",
  }),
  columnHelper.accessor(
    (row) => {
      const sizes = new Set(row.replicas.map((r) => r.size));
      return sizes.size > 0 ? Array.from(sizes).join(", ") : null;
    },
    {
      id: "sizes",
      header: "Size",
      sortingFn: sortingFunctions.nullsLast,
      cell: (info) => info.getValue() ?? "-",
    },
  ),
  columnHelper.accessor("latestStatusUpdate", {
    id: "lastStatusChange",
    header: "Last status change",
    sortingFn: sortingFunctions.nullsLast,
    cell: (info) => {
      const meta = info.table.options.meta as ClusterTableMeta;
      return (
        <LastStatusChangeCell
          cluster={info.row.original}
          offlineReplicaMap={meta.offlineReplicaMap}
        />
      );
    },
  }),
  columnHelper.display({
    id: "actions",
    header: "",
    cell: (info) => {
      const meta = info.table.options.meta as ClusterTableMeta;
      return (
        <ClusterActionsCell
          cluster={info.row.original}
          refetchClusters={meta.refetchClusters}
        />
      );
    },
    enableSorting: false,
    size: OVERFLOW_BUTTON_WIDTH,
  }),
];

const ClustersListContent = ({
  showSystemObjects,
}: {
  showSystemObjects: boolean;
}) => {
  const { data: clusters, refetch } = useClusters({
    includeSystemObjects: showSystemObjects,
  });

  const orderedClusters = React.useMemo(() => {
    if (!clusters) return [];
    const systemClusters = clusters.filter((c) => isSystemCluster(c.id));
    const nonSystemClusters = clusters
      .filter((c) => !isSystemCluster(c.id))
      .sort((a, b) => a.name.localeCompare(b.name));
    return [...nonSystemClusters, ...systemClusters];
  }, [clusters]);

  if (clusters !== null && clusters.length === 0) {
    return (
      <EmptyListWrapper>
        <EmptyListHeader>
          <IconBox type="Empty">
            <ClustersIcon />
          </IconBox>
          <EmptyListHeaderContents
            title="No available clusters"
            helpText="Create a cluster and one or more replicas to enable dataflows."
          />
        </EmptyListHeader>
        <SampleCodeBoxWrapper docsUrl={docUrls["/docs/sql/create-cluster/"]}>
          <CodeBlock
            title={createClusterSuggestion.title}
            contents={createClusterSuggestion.string}
            lineNumbers
          >
            {`CREATE CLUSTER <cluster_name>
  REPLICAS (
    <replica_name> (SIZE = 'xsmall')
);`}
          </CodeBlock>
        </SampleCodeBoxWrapper>
      </EmptyListWrapper>
    );
  }

  return <ClusterTable clusters={orderedClusters} refetchClusters={refetch} />;
};

interface ClusterTableProps {
  clusters: ClusterWithOwnership[];
  refetchClusters: () => void;
}

const ClusterTable = ({ clusters, refetchClusters }: ClusterTableProps) => {
  const { data: offlineReplicaMap, error: offlineReplicaError } =
    useLatestOfflineReplica();

  const meta: ClusterTableMeta = { refetchClusters, offlineReplicaMap };

  const table = useUniversalTable({
    data: clusters,
    columns,
    initialSorting: [{ id: "name", desc: false }],
    pageSize: 20,
    state: {
      columnVisibility: {
        lastStatusChange: !offlineReplicaError,
      },
    },
    meta,
  });

  return (
    <VStack spacing={4} align="stretch">
      <TableSearch
        onValueChange={table.setGlobalFilter}
        placeholder="Search clusters..."
      />
      <UniversalTable
        table={table}
        variant="linkable"
        data-testid="cluster-table"
      />
      <TablePagination table={table} itemLabel="clusters" />
    </VStack>
  );
};

const ClustersListPage = () => {
  const [showSystemObjects, setShowSystemObjects] = useShowSystemObjects();

  return (
    <MainContentContainer>
      <PageHeader>
        <PageHeading>Clusters</PageHeading>
        <HStack spacing={10}>
          <HStack spacing={2}>
            <FormLabel
              htmlFor="show-system-objects"
              variant="inline"
              textStyle="text-base"
            >
              Show system clusters
            </FormLabel>
            <Switch
              id="show-system-objects"
              isChecked={showSystemObjects}
              onChange={() => setShowSystemObjects((value: boolean) => !value)}
            />
          </HStack>
        </HStack>
      </PageHeader>
      <AppErrorBoundary message={CLUSTERS_FETCH_ERROR_MESSAGE}>
        <React.Suspense fallback={<LoadingContainer />}>
          <ClustersListContent showSystemObjects={showSystemObjects} />
        </React.Suspense>
      </AppErrorBoundary>
    </MainContentContainer>
  );
};

export default ClustersListPage;
