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
import {
  ClusterWithOwnership,
  Replica,
} from "~/api/materialize/cluster/clusterList";
import useLatestOfflineReplica, {
  LatestOfflineReplicaMap,
} from "~/api/materialize/cluster/useLatestOfflineReplica";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { CodeBlock } from "~/components/copyableComponents";
import DeleteObjectMenuItem from "~/components/DeleteObjectMenuItem";
import ErrorBox from "~/components/ErrorBox";
import { LoadingContainer } from "~/components/LoadingContainer";
import OverflowMenu, { OVERFLOW_BUTTON_WIDTH } from "~/components/OverflowMenu";
import { sortingFunctions } from "~/components/Table/tableColumnBuilders";
import { TablePagination } from "~/components/Table/TablePagination";
import { TableSearch } from "~/components/Table/TableSearch";
import { UniversalTable } from "~/components/Table/UniversalTable";
import { useUniversalTable } from "~/components/Table/useUniversalTable";
import TextLink from "~/components/TextLink";
import { useFlags } from "~/hooks/useFlags";
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
import { useAllClusters } from "~/store/allClusters";
import WarningIcon from "~/svg/WarningIcon";
import { truncateMaxWidth } from "~/theme/components/Table";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import AlterClusterMenuItem from "./AlterClusterMenuItem";
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "./constants";
import { useOwners } from "./queries";
import { useShowSystemObjects } from "./useShowSystemObjects";

const createClusterSuggestion = {
  title: "Create a cluster",
  string: `CREATE CLUSTER <cluster_name>
  (SIZE = '25cc');`,
};

/**
 * One row in the clusters table: a cluster, or one of its replicas nested
 * underneath it. A union because TanStack's `getSubRows` requires child rows
 * to share the parent's row type.
 */
type ClusterRow =
  | ({ rowType: "cluster" } & ClusterWithOwnership)
  | ({ rowType: "replica" } & Replica);

/**
 * Shared data threaded into cell components via TanStack's table `meta`.
 * Read from `info.table.options.meta` and cast to this shape inside cells.
 */
interface ClusterTableMeta {
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

const ReplicaNameCell = ({ replica }: { replica: Replica }) => (
  <Text textStyle="text-ui-med" noOfLines={1}>
    {replica.name}
  </Text>
);

/** Formats a status-change timestamp for display, or "-" when there is none. */
const formatStatusChange = (timestamp: string | null | undefined) =>
  timestamp ? formatDate(timestamp, FRIENDLY_DATETIME_FORMAT_NO_SECONDS) : "-";

const ReplicaLastStatusChangeCell = ({
  updatedAt,
}: {
  updatedAt: string | null;
}) => <Text noOfLines={1}>{formatStatusChange(updatedAt)}</Text>;

const LastStatusChangeCell = ({
  cluster,
  offlineReplicaMap,
}: {
  cluster: ClusterWithOwnership;
  offlineReplicaMap: LatestOfflineReplicaMap | undefined;
}) => {
  const offlineStatus = offlineReplicaMap?.get(cluster.id);

  const lastStatusChangeString = formatStatusChange(cluster.latestStatusUpdate);

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

const ClusterActionsCell = ({ cluster }: { cluster: ClusterWithOwnership }) => (
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
              // the subscribe drops the row from our list
              onSuccessAction={() => undefined}
              objectType="CLUSTER"
            />
          </>
        ),
      },
    ]}
  />
);

const columnHelper = createColumnHelper<ClusterRow>();

const columns = [
  columnHelper.accessor("name", {
    header: "Name",
    sortingFn: "alphanumeric",
    cell: (info) => {
      const row = info.row.original;
      return row.rowType === "cluster" ? (
        <ClusterNameCell cluster={row} />
      ) : (
        <ReplicaNameCell replica={row} />
      );
    },
    meta: {
      minWidth: { md: "280px", sm: "auto" },
      cellProps: truncateMaxWidth,
    },
  }),
  columnHelper.accessor(
    (row) => (row.rowType === "cluster" ? row.replicas.length : null),
    {
      id: "replicaCount",
      header: "Replicas",
      sortingFn: "basic",
      cell: (info) => info.getValue() ?? "-",
    },
  ),
  columnHelper.accessor(
    (row) => {
      if (row.rowType === "replica") {
        return row.size;
      }
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
  columnHelper.accessor(
    (row) => {
      if (row.rowType === "cluster") {
        return row.latestStatusUpdate;
      }
      // A replica carries one status row per process and the query leaves them
      // unordered, so pick the newest by timestamp.
      return row.statuses.reduce<string | null>(
        (latest, { updated_at }) =>
          latest === null || Date.parse(updated_at) > Date.parse(latest)
            ? updated_at
            : latest,
        null,
      );
    },
    {
      id: "lastStatusChange",
      header: "Last status change",
      sortingFn: sortingFunctions.nullsLast,
      cell: (info) => {
        const row = info.row.original;
        if (row.rowType === "replica") {
          return <ReplicaLastStatusChangeCell updatedAt={info.getValue()} />;
        }
        const meta = info.table.options.meta as ClusterTableMeta;
        return (
          <LastStatusChangeCell
            cluster={row}
            offlineReplicaMap={meta.offlineReplicaMap}
          />
        );
      },
    },
  ),
  columnHelper.display({
    id: "actions",
    header: "",
    cell: (info) => {
      const row = info.row.original;
      return row.rowType === "cluster" ? (
        <ClusterActionsCell cluster={row} />
      ) : (
        ""
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
  const { data: clusters, snapshotComplete, isError } = useAllClusters();
  const { isOwner } = useOwners();

  const orderedClusters = React.useMemo(() => {
    const visibleClusters = clusters
      .filter((c) => showSystemObjects || !isSystemCluster(c.id))
      .map((c) => ({
        rowType: "cluster" as const,
        ...c,
        isOwner: isOwner(c.ownerId),
      }));
    // The subscribe upserts by id, so the atom's order is arbitrary. Sort each
    // group by name and keep system clusters at the end.
    const byName = (a: ClusterWithOwnership, b: ClusterWithOwnership) =>
      a.name.localeCompare(b.name);
    const systemClusters = visibleClusters
      .filter((c) => isSystemCluster(c.id))
      .sort(byName);
    const nonSystemClusters = visibleClusters
      .filter((c) => !isSystemCluster(c.id))
      .sort(byName);
    return [...nonSystemClusters, ...systemClusters];
  }, [clusters, isOwner, showSystemObjects]);

  if (isError) {
    return <ErrorBox message={CLUSTERS_FETCH_ERROR_MESSAGE} />;
  }

  // The atom starts out empty, so the empty state has to wait for the snapshot
  // or it would flash before the first rows arrive.
  if (!snapshotComplete) {
    return <LoadingContainer />;
  }

  if (orderedClusters.length === 0) {
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

  return <ClusterTable clusters={orderedClusters} />;
};

interface ClusterTableProps {
  clusters: ClusterRow[];
}

const ClusterTable = ({ clusters }: ClusterTableProps) => {
  const { data: offlineReplicaMap, error: offlineReplicaError } =
    useLatestOfflineReplica();

  const meta: ClusterTableMeta = { offlineReplicaMap };
  const flags = useFlags();

  const table = useUniversalTable({
    data: clusters,
    columns,
    initialSorting: [{ id: "name", desc: false }],
    pageSize: 20,
    getSubRows: (row) =>
      flags["usage-metrics-in-cluster-list-CNS121"] &&
      row.rowType === "cluster" &&
      row.replicas.length > 0
        ? row.replicas.map((r) => ({ rowType: "replica" as const, ...r }))
        : undefined,
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
        // UniversalTable styles expandable rows as group headings. Cluster rows
        // are ordinary rows that happen to expand, so keep them at the default
        // cell text style, matching their replica rows.
        rowSx={{ td: { textStyle: "text-ui-reg" } }}
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
