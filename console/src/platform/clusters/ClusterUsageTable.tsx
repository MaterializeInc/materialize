// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, Text, Tooltip, VStack } from "@chakra-ui/react";
import {
  ColumnFiltersState,
  createColumnHelper,
  SortingFn,
} from "@tanstack/react-table";
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
import StatusPill from "~/components/StatusPill";
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
import { MultiSelectFilterPanel } from "~/platform/maintained-objects/filterPanels";
import {
  bucketForHydration,
  HYDRATION_BUCKETS,
  HYDRATION_LABELS,
  HydrationBucket,
  STATUS_COLOR_SCHEMES,
} from "~/platform/maintained-objects/filters";
import WarningIcon from "~/svg/WarningIcon";
import { truncateMaxWidth } from "~/theme/components/Table";
import { notNullOrUndefined } from "~/util";
import {
  DATE_FORMAT,
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
  TIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import { ClusterFilterChips } from "./ClusterFilterChips";
import {
  ClusterActionsCell,
  ClusterNameCell,
  ClusterTableMeta,
} from "./clusterTableCells";
import {
  HYDRATION_COLUMN_ID,
  HYDRATION_URL_KEY,
  hydrationFilterFn,
  hydrationFilterFromUrl,
} from "./hydrationFilters";
import {
  OWNER_COLUMN_ID,
  OWNER_URL_KEY,
  ownerFilterFn,
  ownerFilterFromUrl,
} from "./ownerFilters";
import {
  ReplicaHydrationCounts,
  useOwners,
  useReplicaHydration,
  useReplicaUtilization,
} from "./queries";
import { ReplicaCountFilterPanel } from "./ReplicaCountFilterPanel";
import {
  REPLICA_COLUMN_ID,
  REPLICA_COUNT_URL_KEY,
  replicaCountFilterFn,
  replicaCountFilterFromUrl,
  replicaCountFilterToUrl,
} from "./replicaCountFilters";
import { UtilizationFilterPanel } from "./UtilizationFilterPanel";
import {
  utilizationFilterFn,
  utilizationFilterFromUrl,
  utilizationFilterToUrl,
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
 * cluster still gets a row, so every cluster is representable, but the replica
 * count filter defaults to a minimum of one replica and so hides those rows
 * until a reader asks for them. The row model stays complete because the filter
 * is the reader's to remove: it is stated on a chip and is one click from off,
 * which a dropped row could not be. See `replicaCountFilters`.
 *
 * `hydration` is null when the replica has no counted objects, which is not the
 * same as nothing being hydrated. See `buildReplicaHydrationQuery` for what the
 * counts cover.
 */
type ClusterReplicaRow = {
  cluster: ClusterWithOwnership;
  /** The owning role's name, null until the roles load. */
  owner: string | null;
  replica: Replica | null;
  utilization: ReplicaUtilizationValues;
  hydration: ReplicaHydrationCounts | null;
};

const ReplicaPercentCell = ({ value }: { value: number | null }) => {
  // NOTE: an idle replica reports 0, which must render as "0.0%" rather than
  // being treated as "no reading". Only a missing sample is a dash.
  if (value === null) {
    return <>-</>;
  }
  return <PercentBar fraction={value} isStacked />;
};

/**
 * A replica's hydration, or a dash when the counts say nothing about it. A
 * cluster with no replicas and a replica with no counted objects both land
 * here, so the dash reads as "not reported" rather than as "nothing hydrated".
 */
const ReplicaHydrationCell = ({
  bucket,
}: {
  bucket: HydrationBucket | undefined;
}) => {
  if (bucket === undefined) {
    return <>-</>;
  }
  return (
    <StatusPill
      status={bucket}
      label={HYDRATION_LABELS[bucket]}
      colorScheme={STATUS_COLOR_SCHEMES[bucket]}
    />
  );
};

/** How far a replica's hydration has progressed, or null when unknown. */
const hydrationFraction = (counts: ReplicaHydrationCounts | null) =>
  counts && counts.totalObjects > 0
    ? counts.hydratedObjects / counts.totalObjects
    : null;

/**
 * Orders by hydration progress, unknown last. Sorting on the bucket the cell
 * shows would collapse every replica into three ties, so this reaches past it
 * to the counts behind it.
 */
const hydrationSortingFn: SortingFn<ClusterReplicaRow> = (rowA, rowB) => {
  const a = hydrationFraction(rowA.original.hydration);
  const b = hydrationFraction(rowB.original.hydration);

  if (a === null && b === null) return 0;
  if (a === null) return 1;
  if (b === null) return -1;

  return a - b;
};

const ReplicaLastStatusChangeCell = ({
  updatedAt,
  offlineStatus,
}: {
  updatedAt: string | null;
  offlineStatus: LatestOfflineReplicaInfo | undefined;
}) => (
  <HStack>
    {updatedAt ? (
      // The date over its time, so the column is as wide as the date alone.
      <Text as="span" whiteSpace="nowrap">
        <Text as="span" display="block">
          {formatDate(updatedAt, DATE_FORMAT)}
        </Text>{" "}
        <Text
          as="span"
          display="block"
          textStyle="text-small"
          color="foreground.secondary"
        >
          {formatDate(updatedAt, `${TIME_FORMAT_NO_SECONDS} z`)}
        </Text>
      </Text>
    ) : (
      <Text as="span">-</Text>
    )}
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

interface ClusterUsageTableMeta extends ClusterTableMeta {
  /** The roles that own a listed cluster, sorted, for the owner filter. */
  ownerNames: string[];
}

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
  // NOTE: the denominator is the size's configured disk allocation. A size
  // that allocates none but can swap, as self-managed's swap sizes do, shows
  // swap's share of the heap limit instead, as the cluster page does.
  {
    id: "diskPercent",
    header: "Disk",
    urlKey: "disk",
    read: (u) => u.diskPercent ?? u.swapPercent ?? null,
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

/** The column filters a URL asks for, skipping any it cannot parse. */
const columnFiltersFromSearch = (search: string): ColumnFiltersState => {
  const params = new URLSearchParams(search);

  const filters: ColumnFiltersState = UTILIZATION_COLUMNS.flatMap(
    ({ id, urlKey }) => {
      const value = utilizationFilterFromUrl(params.get(urlKey));
      return value ? [{ id, value }] : [];
    },
  );

  const buckets = hydrationFilterFromUrl(params.getAll(HYDRATION_URL_KEY));
  if (buckets) {
    filters.push({ id: HYDRATION_COLUMN_ID, value: buckets });
  }

  const minimumReplicas = replicaCountFilterFromUrl(
    params.get(REPLICA_COUNT_URL_KEY),
  );
  if (minimumReplicas !== undefined) {
    filters.push({ id: REPLICA_COLUMN_ID, value: minimumReplicas });
  }

  const owners = ownerFilterFromUrl(params.getAll(OWNER_URL_KEY));
  if (owners) {
    filters.push({ id: OWNER_COLUMN_ID, value: owners });
  }

  return filters;
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
    meta: {
      renderFilter: (column) => (
        <UtilizationFilterPanel column={column} label={header} />
      ),
    },
  });

const columns = [
  columnHelper.accessor((row) => row.cluster.name, {
    id: "cluster",
    header: "Cluster",
    sortingFn: "alphanumeric",
    cell: (info) => <ClusterNameCell cluster={info.row.original.cluster} />,
    meta: {
      minWidth: { md: "140px", sm: "auto" },
      cellProps: { maxW: { base: "120px", xl: "150px", "2xl": "400px" } },
    },
  }),
  columnHelper.accessor((row) => row.replica?.name ?? null, {
    id: REPLICA_COLUMN_ID,
    header: "Replica",
    sortingFn: sortingFunctions.nullsLast,
    cell: (info) => info.getValue() ?? "-",
    // The filter counts the row's cluster's replicas, so it hides the rows of
    // whole clusters rather than individual replicas.
    filterFn: replicaCountFilterFn,
    meta: {
      cellProps: truncateMaxWidth,
      renderFilter: (column) => <ReplicaCountFilterPanel column={column} />,
    },
  }),
  columnHelper.accessor((row) => row.replica?.size ?? null, {
    id: "size",
    header: "Size",
    sortingFn: sortingFunctions.nullsLast,
    sortDescFirst: true,
    cell: (info) => info.getValue() ?? "-",
  }),
  columnHelper.accessor((row) => row.owner, {
    id: OWNER_COLUMN_ID,
    header: "Owner",
    sortingFn: sortingFunctions.nullsLast,
    filterFn: ownerFilterFn,
    cell: (info) => {
      const owner = info.getValue();
      // Role names are often email addresses. A long one is cut short, with
      // the whole name on hover.
      return owner === null ? (
        "-"
      ) : (
        <Text as="span" noOfLines={1} title={owner}>
          {owner}
        </Text>
      );
    },
    meta: {
      cellProps: { maxW: { base: "110px", xl: "120px", "2xl": "240px" } },
      renderFilter: (column, table) => {
        const meta = table.options.meta as ClusterUsageTableMeta;
        return (
          <MultiSelectFilterPanel<string, ClusterReplicaRow>
            column={column}
            items={meta.ownerNames}
            searchable
            searchPlaceholder="Search roles..."
            emptyMessage="No roles match"
          />
        );
      },
    },
  }),
  ...UTILIZATION_COLUMNS.map(percentColumn),
  columnHelper.accessor(
    (row) =>
      row.hydration
        ? bucketForHydration(
            row.hydration.hydratedObjects,
            row.hydration.totalObjects,
          )
        : undefined,
    {
      id: HYDRATION_COLUMN_ID,
      header: "Hydration",
      sortingFn: hydrationSortingFn,
      sortDescFirst: false,
      filterFn: hydrationFilterFn,
      enableGlobalFilter: false,
      cell: (info) => <ReplicaHydrationCell bucket={info.getValue()} />,
      meta: {
        tooltip:
          "Whether the objects on this replica have finished reading their history.",
        renderFilter: (column) => (
          <MultiSelectFilterPanel<HydrationBucket, ClusterReplicaRow>
            column={column}
            items={HYDRATION_BUCKETS}
            getLabel={(bucket) => HYDRATION_LABELS[bucket]}
          />
        ),
      },
    },
  ),
  columnHelper.accessor((row) => latestReplicaStatusAt(row.replica), {
    // NOTE: deliberately not the cluster's own `latestStatusUpdate`. That comes
    // from the replica status *history*, so it counts replicas that have since
    // been dropped and can name a time no visible row reports.
    id: "lastStatusChange",
    // Wraps, as its dates do: on one line this is the widest column.
    header: () => (
      <Text as="span" whiteSpace="normal" minWidth="80px">
        Last status change
      </Text>
    ),
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
  const { data: replicaHydration } = useReplicaHydration();
  const { ownerName } = useOwners();
  const location = useLocation();

  // Read once, on mount: the URL seeds the table, and from then on the table
  // drives the URL. Reading it on every render would fight the writer below.
  const [initialState] = React.useState(() =>
    getInitialTableState(location.search),
  );
  const [columnFilters, setColumnFilters] = React.useState<ColumnFiltersState>(
    () => columnFiltersFromSearch(location.search),
  );

  // TanStack recomputes its row models whenever `data` changes identity, so the
  // flattened rows have to outlive the render that built them.
  const rows = React.useMemo<ClusterReplicaRow[]>(
    () =>
      clusters.flatMap((cluster): ClusterReplicaRow[] => {
        const owner = ownerName(cluster.ownerId) ?? null;
        return cluster.replicas.length === 0
          ? [
              {
                cluster,
                owner,
                replica: null,
                utilization: NO_UTILIZATION,
                hydration: null,
              },
            ]
          : cluster.replicas.map((replica) => ({
              cluster,
              owner,
              replica,
              utilization:
                replicaUtilization?.get(replica.id) ?? NO_UTILIZATION,
              hydration: replicaHydration?.get(replica.id) ?? null,
            }));
      }),
    [clusters, ownerName, replicaHydration, replicaUtilization],
  );

  const meta = React.useMemo<ClusterUsageTableMeta>(
    () => ({
      offlineReplicaMap,
      ownerNames: [
        ...new Set(rows.map(({ owner }) => owner).filter(notNullOrUndefined)),
      ].sort((a, b) => a.localeCompare(b)),
    }),
    [offlineReplicaMap, rows],
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
        params[urlKey] = utilizationFilterToUrl(filter.value as number);
      }
    }
    const hydrationFilter = tableState.columnFilters.find(
      (filter) => filter.id === HYDRATION_COLUMN_ID,
    );
    if (hydrationFilter) {
      params[HYDRATION_URL_KEY] = hydrationFilter.value;
    }
    const replicaCountFilter = tableState.columnFilters.find(
      (filter) => filter.id === REPLICA_COLUMN_ID,
    );
    const minimumReplicas = replicaCountFilterToUrl(
      replicaCountFilter?.value as number | undefined,
    );
    if (minimumReplicas !== undefined) {
      params[REPLICA_COUNT_URL_KEY] = minimumReplicas;
    }
    const ownerFilter = tableState.columnFilters.find(
      (filter) => filter.id === OWNER_COLUMN_ID,
    );
    if (ownerFilter) {
      params[OWNER_URL_KEY] = ownerFilter.value;
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

  // Every cluster contributes at least one row, and the list renders its own
  // empty state when there are no clusters at all, so an empty row model here
  // means the search or a filter excluded everything.
  const noMatches = table.getFilteredRowModel().rows.length === 0;

  return (
    <VStack spacing={4} align="stretch">
      <TableSearch
        initialValue={initialState.globalFilter ?? ""}
        onValueChange={table.setGlobalFilter}
        placeholder="Search clusters..."
      />
      {/*
       * Above the table, so it outlives an empty result. The message below
       * replaces the table and takes the column headers, and so the filter
       * panels, with it. Removing a chip is then the only way back from a
       * filter that matched nothing.
       */}
      <ClusterFilterChips
        table={table}
        utilizationColumns={UTILIZATION_COLUMNS}
      />
      {noMatches ? (
        <EmptyListWrapper>
          <EmptyListHeader>
            <EmptyListHeaderContents
              title="No replicas match the current search and filters"
              helpText="Try a different search term, or remove a filter above."
            />
          </EmptyListHeader>
        </EmptyListWrapper>
      ) : (
        <>
          {/* Eleven columns at the table's usual padding overflow a laptop
              window, so cells here are padded less. */}
          <Box sx={{ "& th, & td": { px: 3 } }}>
            <UniversalTable
              table={table}
              variant="linkable"
              data-testid="cluster-table"
            />
          </Box>
          <TablePagination table={table} itemLabel="replicas" />
        </>
      )}
    </VStack>
  );
};
