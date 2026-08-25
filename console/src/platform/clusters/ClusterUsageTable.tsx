// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Text, Tooltip, VStack } from "@chakra-ui/react";
import { ColumnFiltersState, createColumnHelper } from "@tanstack/react-table";
import React from "react";
import { useLocation } from "react-router-dom";

import {
  ClusterWithOwnership,
  Replica,
} from "~/api/materialize/cluster/clusterList";
import { ReplicaUtilization } from "~/api/materialize/cluster/replicaUtilization";
import useLatestOfflineReplica, {
  LatestOfflineReplicaInfo,
} from "~/api/materialize/cluster/useLatestOfflineReplica";
import { OVERFLOW_BUTTON_WIDTH } from "~/components/OverflowMenu";
import PercentBar from "~/components/PercentBar";
import { sortingFunctions } from "~/components/Table/tableColumnBuilders";
import { TablePagination } from "~/components/Table/TablePagination";
import { TableSearch } from "~/components/Table/TableSearch";
import { UniversalTable } from "~/components/Table/UniversalTable";
import {
  getInitialTableState,
  useUniversalTable,
} from "~/components/Table/useUniversalTable";
import { useSyncObjectToSearchParams } from "~/hooks/useSyncObjectToSearchParams";
import {
  EmptyListHeader,
  EmptyListHeaderContents,
  EmptyListWrapper,
} from "~/layouts/listPageComponents";
import WarningIcon from "~/svg/WarningIcon";
import { truncateMaxWidth } from "~/theme/components/Table";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import {
  ClusterActionsCell,
  ClusterNameCell,
  ClusterTableMeta,
} from "./clusterTableCells";
import { useReplicaUtilization } from "./queries";
import { UtilizationFilter } from "./UtilizationFilter";
import {
  utilizationFilterFn,
  utilizationFilterFromUrl,
  utilizationFilterToUrl,
  UtilizationFilterValue,
} from "./utilizationFilters";

/**
 * The utilization readings a row displays, as fractions of the replica's
 * allocation. Null for a replica with no sample in the window.
 */
type ReplicaUtilizationValues = Omit<ReplicaUtilization, "replicaId">;

const NO_UTILIZATION: ReplicaUtilizationValues = {
  cpuPercent: null,
  memoryPercent: null,
  diskPercent: null,
  heapPercent: null,
};

/**
 * One row: a replica, together with the cluster it belongs to and its
 * utilization.
 *
 * Utilization arrives from a separate query than the replica itself, so it is
 * merged onto the row before the table sees it. Column accessors are handed the
 * row and nothing else, so a reading they sort on has to live here rather than
 * be looked up from table meta.
 *
 * `replica` is null for a cluster that currently has no replicas. Such a
 * cluster still gets a row, so the list stays a complete inventory of clusters
 * rather than silently hiding the ones with nothing running.
 */
type ClusterReplicaRow = {
  cluster: ClusterWithOwnership;
  replica: Replica | null;
  utilization: ReplicaUtilizationValues;
};

const ReplicaPercentCell = ({ value }: { value: number | null }) => {
  // NOTE: an idle replica reports 0, which must render as "0.0%" rather than
  // being treated as "no reading". Only a missing sample is a dash.
  if (value === null) {
    return <>-</>;
  }
  return <PercentBar fraction={value} />;
};

/** Formats a status-change timestamp for display, or "-" when there is none. */
const formatStatusChange = (timestamp: string | null | undefined) =>
  timestamp ? formatDate(timestamp, FRIENDLY_DATETIME_FORMAT_NO_SECONDS) : "-";

const ReplicaLastStatusChangeCell = ({
  updatedAt,
  offlineStatus,
}: {
  updatedAt: string | null;
  offlineStatus: LatestOfflineReplicaInfo | undefined;
}) => (
  <HStack>
    <Text as="span" noOfLines={1}>
      {formatStatusChange(updatedAt)}
    </Text>
    {offlineStatus?.shouldSurfaceOom && (
      <Tooltip
        px={3}
        py={2}
        minWidth="fit-content"
        rounded="md"
        label={`This replica ran out of memory on ${formatDate(
          offlineStatus.lastOfflineAt,
          FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
        )}`}
      >
        <WarningIcon
          // The tooltip text is only reachable on hover, so the icon needs a
          // name of its own to mean anything to a screen reader.
          role="img"
          aria-label="Ran out of memory"
        />
      </Tooltip>
    )}
  </HStack>
);

/**
 * The newest of a replica's status timestamps, or null when it has none. A
 * replica carries one status row per process and the query leaves them
 * unordered, so position must not decide which one wins.
 */
const latestReplicaStatusAt = (replica: Replica | null) =>
  replica?.statuses.reduce<string | null>(
    (latest, { updated_at }) =>
      latest === null || Date.parse(updated_at) > Date.parse(latest)
        ? updated_at
        : latest,
    null,
  ) ?? null;

const columnHelper = createColumnHelper<ClusterReplicaRow>();

interface UtilizationColumn {
  id: string;
  header: string;
  urlKey: string;
  read: (utilization: ReplicaUtilizationValues) => number | null;
}

/**
 * The utilization columns, in the order the table shows them. One list defines
 * both the columns and the toolbar filter controls, so a control cannot end up
 * labelled differently from the column it filters.
 */
const UTILIZATION_COLUMNS: UtilizationColumn[] = [
  { id: "cpuPercent", header: "CPU", urlKey: "cpu", read: (u) => u.cpuPercent },
  // NOTE: this is `memory_percent`, RAM against the size's RAM allocation.
  {
    id: "memoryPercent",
    header: "Memory",
    urlKey: "memory",
    read: (u) => u.memoryPercent,
  },
  // NOTE: the denominator is the size's configured disk allocation, so this is
  // null, and renders a dash, for any replica on a size that allocates no disk.
  {
    id: "diskPercent",
    header: "Disk",
    urlKey: "disk",
    read: (u) => u.diskPercent,
  },
  // NOTE: `heap_percent` is RAM plus swap over the heap limit. The heap limit
  // comes from the orchestrator, not the size catalog, so this is null on any
  // environment that does not report one.
  {
    id: "heapPercent",
    header: "Heap",
    urlKey: "heap",
    read: (u) => u.heapPercent,
  },
];

/** The utilization filters a URL asks for, skipping any it cannot parse. */
const utilizationFiltersFromSearch = (search: string): ColumnFiltersState => {
  const params = new URLSearchParams(search);
  return UTILIZATION_COLUMNS.flatMap(({ id, urlKey }) => {
    const value = utilizationFilterFromUrl(params.get(urlKey));
    return value ? [{ id, value }] : [];
  });
};

/** A utilization column, read from the row's readings by `read`. */
const percentColumn = ({ id, header, read }: UtilizationColumn) =>
  columnHelper.accessor((row) => read(row.utilization), {
    id,
    header,
    sortingFn: sortingFunctions.numericNullsLast,
    sortDescFirst: true,
    filterFn: utilizationFilterFn,
    cell: (info) => <ReplicaPercentCell value={info.getValue()} />,
  });

const columns = [
  columnHelper.accessor((row) => row.cluster.name, {
    id: "cluster",
    header: "Cluster",
    sortingFn: "alphanumeric",
    cell: (info) => <ClusterNameCell cluster={info.row.original.cluster} />,
    meta: {
      minWidth: { md: "240px", sm: "auto" },
      cellProps: truncateMaxWidth,
    },
  }),
  columnHelper.accessor((row) => row.replica?.name ?? null, {
    id: "replica",
    header: "Replica",
    sortingFn: sortingFunctions.nullsLast,
    cell: (info) => info.getValue() ?? "-",
    meta: {
      cellProps: truncateMaxWidth,
    },
  }),
  columnHelper.accessor((row) => row.replica?.size ?? null, {
    id: "size",
    header: "Size",
    sortingFn: sortingFunctions.nullsLast,
    sortDescFirst: true,
    cell: (info) => info.getValue() ?? "-",
  }),
  ...UTILIZATION_COLUMNS.map(percentColumn),
  columnHelper.accessor((row) => latestReplicaStatusAt(row.replica), {
    // NOTE: deliberately not the cluster's own `latestStatusUpdate`. That comes
    // from the replica status *history*, so it counts replicas that have since
    // been dropped and can name a time no visible row reports.
    id: "lastStatusChange",
    header: "Last status change",
    sortingFn: sortingFunctions.nullsLast,
    sortDescFirst: false,
    cell: (info) => {
      const { replica } = info.row.original;
      const meta = info.table.options.meta as ClusterTableMeta;
      return (
        <ReplicaLastStatusChangeCell
          updatedAt={info.getValue()}
          offlineStatus={
            replica ? meta.offlineReplicaMap?.get(replica.id) : undefined
          }
        />
      );
    },
  }),
  columnHelper.display({
    id: "actions",
    header: "",
    // The menu acts on the cluster, so every replica row of a cluster offers
    // the same actions. Its items name their subject ("Drop cluster").
    cell: (info) => <ClusterActionsCell cluster={info.row.original.cluster} />,
    enableSorting: false,
    size: OVERFLOW_BUTTON_WIDTH,
  }),
];

const PAGE_SIZE = 20;

export interface ClusterUsageTableProps {
  clusters: ClusterWithOwnership[];
}

/**
 * One row per replica, carrying its cluster, size, utilization, and status.
 */
export const ClusterUsageTable = ({ clusters }: ClusterUsageTableProps) => {
  const { data: offlineReplicaMap, error: offlineReplicaError } =
    useLatestOfflineReplica();
  const { data: replicaUtilization } = useReplicaUtilization();
  const location = useLocation();

  const meta: ClusterTableMeta = { offlineReplicaMap };

  // Read once, on mount: the URL seeds the table, and from then on the table
  // drives the URL. Reading it on every render would fight the writer below.
  const [initialState] = React.useState(() =>
    getInitialTableState(location.search),
  );
  const [columnFilters, setColumnFilters] = React.useState<ColumnFiltersState>(
    () => utilizationFiltersFromSearch(location.search),
  );

  // TanStack recomputes its row models whenever `data` changes identity, so the
  // flattened rows have to outlive the render that built them.
  const rows = React.useMemo<ClusterReplicaRow[]>(
    () =>
      clusters.flatMap((cluster): ClusterReplicaRow[] =>
        cluster.replicas.length === 0
          ? [{ cluster, replica: null, utilization: NO_UTILIZATION }]
          : cluster.replicas.map((replica) => ({
              cluster,
              replica,
              utilization:
                replicaUtilization?.get(replica.id) ?? NO_UTILIZATION,
            })),
      ),
    [clusters, replicaUtilization],
  );

  const table = useUniversalTable({
    data: rows,
    columns,
    getRowId: (row) =>
      `${row.cluster.id}/${row.replica ? row.replica.id : "no-replica"}`,
    initialSorting: initialState.sorting ?? [{ id: "cluster", desc: false }],
    pageSize: PAGE_SIZE,
    initialState: {
      globalFilter: initialState.globalFilter,
      pagination: {
        pageIndex: initialState.pageIndex ?? 0,
        pageSize: PAGE_SIZE,
      },
    },
    state: {
      columnFilters,
      columnVisibility: {
        lastStatusChange: !offlineReplicaError,
      },
    },
    onColumnFiltersChange: setColumnFilters,
    meta,
  });

  const tableState = table.getState();

  // The whole query string is rewritten from this object, so it has to carry
  // every piece of table state worth bookmarking, not just the filters. Keys
  // are left out when they hold nothing, to keep a plain visit to the page from
  // accumulating empty parameters.
  const urlParams = React.useMemo(() => {
    const params: Record<string, unknown> = {};
    for (const { id, urlKey } of UTILIZATION_COLUMNS) {
      const filter = tableState.columnFilters.find((f) => f.id === id);
      if (filter) {
        params[urlKey] = utilizationFilterToUrl(
          filter.value as UtilizationFilterValue,
        );
      }
    }
    if (tableState.globalFilter) {
      params.q = tableState.globalFilter;
    }
    const [sort] = tableState.sorting;
    if (sort) {
      params.sort = sort.id;
      params.dir = sort.desc ? "desc" : "asc";
    }
    if (tableState.pagination.pageIndex > 0) {
      params.page = tableState.pagination.pageIndex + 1;
    }
    return params;
  }, [
    tableState.columnFilters,
    tableState.globalFilter,
    tableState.sorting,
    tableState.pagination.pageIndex,
  ]);
  useSyncObjectToSearchParams(urlParams);

  const noMatches = table.getFilteredRowModel().rows.length === 0;

  return (
    <VStack spacing={4} align="stretch">
      <HStack spacing={3} flexWrap="wrap">
        <TableSearch
          initialValue={initialState.globalFilter ?? ""}
          onValueChange={table.setGlobalFilter}
          placeholder="Search clusters..."
        />
        {UTILIZATION_COLUMNS.map(({ id, header }) => {
          const column = table.getColumn(id);
          return (
            column && (
              <UtilizationFilter key={id} column={column} label={header} />
            )
          );
        })}
      </HStack>
      {noMatches ? (
        <EmptyListWrapper>
          <EmptyListHeader>
            <EmptyListHeaderContents
              title="No replicas match the current search and filters"
              helpText="Try a different search term, or clear the utilization filters."
            />
          </EmptyListHeader>
        </EmptyListWrapper>
      ) : (
        <>
          <UniversalTable
            table={table}
            variant="linkable"
            data-testid="cluster-table"
          />
          <TablePagination table={table} itemLabel="replicas" />
        </>
      )}
    </VStack>
  );
};
