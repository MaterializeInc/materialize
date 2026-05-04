// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  HStack,
  Skeleton,
  Spinner,
  Text,
  Tooltip,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import {
  ColumnFiltersState,
  createColumnHelper,
  OnChangeFn,
  PaginationState,
} from "@tanstack/react-table";
import React from "react";
import { Link as RouterLink, useLocation } from "react-router-dom";

import { createNamespace } from "~/api/materialize";
import { OUTDATED_THRESHOLD_SECONDS } from "~/api/materialize/cluster/materializationLag";
import { sortingFunctions } from "~/components/Table/tableColumnBuilders";
import { TablePagination } from "~/components/Table/TablePagination";
import { TableSearch } from "~/components/Table/TableSearch";
import { UniversalTable } from "~/components/Table/UniversalTable";
import {
  getInitialTableState,
  useUniversalTable,
} from "~/components/Table/useUniversalTable";
import TextLink from "~/components/TextLink";
import TimePeriodSelect from "~/components/TimePeriodSelect";
import { useSyncObjectToSearchParams } from "~/hooks/useSyncObjectToSearchParams";
import { useTimePeriodMinutes } from "~/hooks/useTimePeriodSelect";
import {
  MainContentContainer,
  PageHeader,
  PageHeading,
} from "~/layouts/BaseLayout";
import {
  EmptyListHeader,
  EmptyListHeaderContents,
  EmptyListWrapper,
} from "~/layouts/listPageComponents";
import { absoluteClusterPath } from "~/platform/routeHelpers";
import { useRegionSlug } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";
import { pluralize } from "~/util";
import { formatIntervalShort } from "~/utils/format";

import { FilterChips } from "./FilterChips";
import {
  ClusterFilterPanel,
  FreshnessFilterPanel,
  HydrationFilterPanel,
  ObjectTypeFilterPanel,
} from "./filterPanels";
import {
  bucketForHydration,
  clusterFilterFn,
  FILTER_URL_SPECS,
  freshnessFilterFn,
  HYDRATION_LABELS,
  hydrationFilterFn,
  initialColumnFiltersFromUrl,
  objectTypeFilterFn,
} from "./filters";
import { MaintainedObjectListItem, useMaintainedObjectsList } from "./queries";

const PAGE_SIZE = 25;

// `mz_wallclock_global_lag_recent_history` retains up to 24 hours.
const LOOKBACK_OPTIONS: Record<string, string> = {
  "1": "Past 1 minute",
  "5": "Past 5 minutes",
  "15": "Past 15 minutes",
  "30": "Past 30 minutes",
  "60": "Past 1 hour",
  "180": "Past 3 hours",
  "360": "Past 6 hours",
  "1440": "Past 24 hours",
};

const ObjectNameCell = ({ row }: { row: MaintainedObjectListItem }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const namespace = createNamespace(row.databaseName, row.schemaName);
  const fullyQualified = namespace ? `${namespace}.${row.name}` : row.name;
  return (
    <Tooltip label={fullyQualified} lineHeight={1.2} openDelay={200}>
      <Box>
        <Text
          textStyle="text-small"
          fontWeight="500"
          noOfLines={1}
          color={colors.foreground.secondary}
        >
          {namespace}
        </Text>
        <Text textStyle="text-ui-med" noOfLines={1}>
          {row.name}
        </Text>
      </Box>
    </Tooltip>
  );
};

const OUTDATED_MS = OUTDATED_THRESHOLD_SECONDS * 1_000;
const WARNING_MS = OUTDATED_MS / 2;

interface MaintainedObjectsTableMeta {
  lagReady: boolean;
  hydrationReady: boolean;
  regionSlug: string;
  clusterNames: string[];
}

const ClusterCell = ({
  row,
  regionSlug,
}: {
  row: MaintainedObjectListItem;
  regionSlug: string;
}) => {
  if (!row.clusterId || !row.clusterName) return "-";
  const path = absoluteClusterPath(regionSlug, {
    id: row.clusterId,
    name: row.clusterName,
  });
  return (
    <TextLink as={RouterLink} to={path} textStyle="text-ui-med" noOfLines={1}>
      {row.clusterName}
    </TextLink>
  );
};

const FreshnessCell = ({
  row,
  ready,
}: {
  row: MaintainedObjectListItem;
  ready: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  if (!row.lag || row.lagMs === null) {
    return ready ? "-" : <Skeleton height="3" width="14" />;
  }
  const color =
    row.lagMs >= OUTDATED_MS
      ? colors.accent.red
      : row.lagMs >= WARNING_MS
        ? colors.accent.orange
        : colors.foreground.primary;
  return (
    <Text textStyle="text-ui-med" color={color}>
      {formatIntervalShort(
        row.lag as Parameters<typeof formatIntervalShort>[0],
      )}
    </Text>
  );
};

const HydrationCell = ({
  row,
  ready,
}: {
  row: MaintainedObjectListItem;
  ready: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { hydratedReplicas, totalReplicas } = row;
  if (totalReplicas === 0) {
    return ready ? "-" : <Skeleton height="2" width="20" borderRadius="full" />;
  }
  const bucket = bucketForHydration(hydratedReplicas, totalReplicas);
  const percent = Math.round((hydratedReplicas / totalReplicas) * 100);
  const labelColor =
    bucket === "hydrated"
      ? colors.accent.green
      : bucket === "not_hydrated"
        ? colors.accent.red
        : colors.accent.orange;
  const barColor =
    bucket === "hydrated" ? colors.accent.green : colors.accent.orange;
  const tooltip = `${hydratedReplicas} of ${totalReplicas} ${pluralize(
    totalReplicas,
    "replica",
    "replicas",
  )} hydrated`;
  return (
    <Tooltip label={tooltip} lineHeight={1.2}>
      <HStack spacing="2">
        <Box
          width="12"
          height="2"
          borderRadius="full"
          bg={colors.background.secondary}
          overflow="hidden"
          flexShrink={0}
        >
          <Box
            height="100%"
            width={`${percent}%`}
            borderRadius="full"
            bg={barColor}
            transition="width 0.3s ease"
          />
        </Box>
        <Text textStyle="text-ui-med" whiteSpace="nowrap" color={labelColor}>
          {bucket && HYDRATION_LABELS[bucket]}
        </Text>
        <Text
          textStyle="text-ui-reg"
          whiteSpace="nowrap"
          color={colors.foreground.secondary}
        >
          {hydratedReplicas}/{totalReplicas}{" "}
          {pluralize(totalReplicas, "replica", "replicas")}
        </Text>
      </HStack>
    </Tooltip>
  );
};

const columnHelper = createColumnHelper<MaintainedObjectListItem>();

const columns = [
  columnHelper.accessor("name", {
    id: "name",
    header: "Object Name",
    sortingFn: "alphanumeric",
    cell: (info) => <ObjectNameCell row={info.row.original} />,
    meta: {
      minWidth: { md: "280px", sm: "auto" },
      cellProps: { ...truncateMaxWidth, py: "2" },
    },
  }),
  columnHelper.accessor("objectType", {
    id: "objectType",
    header: "Object Type",
    sortingFn: "alphanumeric",
    filterFn: objectTypeFilterFn,
    cell: (info) => (
      <Text textStyle="text-ui-reg">{info.getValue() ?? "-"}</Text>
    ),
    meta: {
      renderFilter: (column) => <ObjectTypeFilterPanel column={column} />,
    },
  }),
  columnHelper.accessor((row) => row.lagMs, {
    id: "freshness",
    header: "Freshness (pMAX)",
    sortingFn: sortingFunctions.nullsLast,
    filterFn: freshnessFilterFn,
    cell: (info) => {
      const meta = info.table.options.meta as MaintainedObjectsTableMeta;
      return <FreshnessCell row={info.row.original} ready={meta.lagReady} />;
    },
    meta: {
      tooltip: `pMAX lag aggregated within the selected time period. Outdated above ${OUTDATED_THRESHOLD_SECONDS}s.`,
      renderFilter: (column) => <FreshnessFilterPanel column={column} />,
    },
  }),
  columnHelper.accessor(
    (row) =>
      row.totalReplicas === 0 ? null : row.hydratedReplicas / row.totalReplicas,
    {
      id: "hydration",
      header: "Hydration",
      sortingFn: sortingFunctions.nullsLast,
      filterFn: hydrationFilterFn,
      cell: (info) => {
        const meta = info.table.options.meta as MaintainedObjectsTableMeta;
        return (
          <HydrationCell row={info.row.original} ready={meta.hydrationReady} />
        );
      },
      meta: {
        tooltip:
          "Fraction of replicas reporting hydrated for this object across all clusters.",
        renderFilter: (column) => <HydrationFilterPanel column={column} />,
      },
    },
  ),
  columnHelper.accessor("clusterName", {
    id: "clusterName",
    header: "Cluster",
    sortingFn: sortingFunctions.nullsLast,
    filterFn: clusterFilterFn,
    cell: (info) => {
      const meta = info.table.options.meta as MaintainedObjectsTableMeta;
      return (
        <ClusterCell row={info.row.original} regionSlug={meta.regionSlug} />
      );
    },
    meta: {
      renderFilter: (column, table) => {
        const meta = table.options.meta as MaintainedObjectsTableMeta;
        return (
          <ClusterFilterPanel column={column} clusters={meta.clusterNames} />
        );
      },
    },
  }),
];

const MaintainedObjects = () => {
  const { colors } = useTheme<MaterializeTheme>();
  const location = useLocation();
  const regionSlug = useRegionSlug();

  const [timePeriodMinutes, setTimePeriodMinutes] = useTimePeriodMinutes({
    localStorageKey: "maintained-objects-time-period",
    defaultValue: "15",
    timePeriodOptions: LOOKBACK_OPTIONS,
  });

  const {
    data: objects,
    isLoading,
    lagReady,
    hydrationReady,
  } = useMaintainedObjectsList({
    lookbackMinutes: timePeriodMinutes,
  });

  const [initialState] = React.useState(() =>
    getInitialTableState(location.search),
  );
  const [columnFilters, setColumnFiltersState] =
    React.useState<ColumnFiltersState>(() =>
      initialColumnFiltersFromUrl(location.search),
    );
  const [pagination, setPagination] = React.useState<PaginationState>(() => ({
    pageIndex: initialState.pageIndex ?? 0,
    pageSize: PAGE_SIZE,
  }));

  const setColumnFilters: OnChangeFn<ColumnFiltersState> = (updater) => {
    setColumnFiltersState(updater);
    setPagination((p) => ({ ...p, pageIndex: 0 }));
  };

  const clusterNames = React.useMemo(() => {
    if (!objects) return [];
    const names = new Set<string>();
    for (const obj of objects) {
      if (obj.clusterName) names.add(obj.clusterName);
    }
    return [...names].sort();
  }, [objects]);

  const table = useUniversalTable({
    data: objects ?? [],
    columns,
    state: { columnFilters, pagination },
    onColumnFiltersChange: setColumnFilters,
    onPaginationChange: setPagination,
    initialSorting: initialState.sorting ?? [{ id: "name", desc: false }],
    initialState: {
      globalFilter: initialState.globalFilter,
    },
    pageSize: PAGE_SIZE,
    meta: {
      lagReady,
      hydrationReady,
      regionSlug,
      clusterNames,
    } satisfies MaintainedObjectsTableMeta,
  });

  const tableState = table.getState();
  const urlParamObject = React.useMemo(() => {
    const obj: Record<string, unknown> = {};
    for (const filter of tableState.columnFilters) {
      const spec = FILTER_URL_SPECS.find((s) => s.columnId === filter.id);
      const encoded = spec?.toUrl(filter.value);
      if (encoded) obj[encoded.key] = encoded.value;
    }
    if (tableState.globalFilter) obj.q = tableState.globalFilter;
    const sort = tableState.sorting[0];
    if (sort) {
      obj.sort = sort.id;
      obj.dir = sort.desc ? "desc" : "asc";
    }
    if (tableState.pagination.pageIndex > 0) {
      obj.page = tableState.pagination.pageIndex + 1;
    }
    return obj;
  }, [
    tableState.columnFilters,
    tableState.globalFilter,
    tableState.sorting,
    tableState.pagination.pageIndex,
  ]);
  useSyncObjectToSearchParams(urlParamObject);

  if (isLoading) {
    return (
      <MainContentContainer>
        <PageHeader boxProps={{ mb: "4" }}>
          <PageHeading>Maintained Objects</PageHeading>
        </PageHeader>
        <Spinner data-testid="loading-spinner" />
      </MainContentContainer>
    );
  }

  const filteredCount = table.getFilteredRowModel().rows.length;
  const totalCount = objects?.length ?? 0;
  const hasFilters = tableState.columnFilters.length > 0;
  const showEmpty = objects && filteredCount === 0;

  return (
    <MainContentContainer>
      <PageHeader boxProps={{ mb: "4" }}>
        <PageHeading>Maintained Objects</PageHeading>
      </PageHeader>

      <VStack alignItems="stretch" width="100%" spacing="4">
        <HStack
          width="100%"
          justifyContent="space-between"
          flexWrap="wrap"
          gap="3"
        >
          <HStack spacing="3" flexWrap="wrap">
            <TimePeriodSelect
              timePeriodMinutes={timePeriodMinutes}
              setTimePeriodMinutes={setTimePeriodMinutes}
              options={LOOKBACK_OPTIONS}
            />
          </HStack>
          <TableSearch
            initialValue={initialState.globalFilter ?? ""}
            onValueChange={table.setGlobalFilter}
            placeholder="Search objects..."
          />
        </HStack>

        {hasFilters && <FilterChips table={table} />}

        {showEmpty ? (
          <EmptyListWrapper>
            <EmptyListHeader>
              <EmptyListHeaderContents
                title="No objects match the selected filters"
                helpText="Try adjusting your filters or time period."
              />
            </EmptyListHeader>
          </EmptyListWrapper>
        ) : (
          <>
            <UniversalTable
              table={table}
              variant="linkable"
              rowSx={{ cursor: "pointer" }}
              data-testid="maintained-objects-table"
            />
            <TablePagination table={table} itemLabel="objects" />
            <Text
              textStyle="text-ui-reg"
              color={colors.foreground.secondary}
              alignSelf="flex-end"
            >
              {filteredCount} of {totalCount} objects
            </Text>
          </>
        )}
      </VStack>
    </MainContentContainer>
  );
};

export { MaintainedObjects };
export default MaintainedObjects;
