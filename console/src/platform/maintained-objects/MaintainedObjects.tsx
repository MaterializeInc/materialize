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
  Button,
  HStack,
  Menu,
  MenuButton,
  MenuItem,
  MenuList,
  Skeleton,
  Spinner,
  Tag,
  TagCloseButton,
  TagLabel,
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
import { Link as RouterLink, useLocation, useNavigate } from "react-router-dom";

import { createNamespace } from "~/api/materialize";
import { OUTDATED_THRESHOLD_SECONDS } from "~/api/materialize/cluster/materializationLag";
import StatusPill from "~/components/StatusPill";
import { sortingFunctions } from "~/components/Table/tableColumnBuilders";
import { TablePagination } from "~/components/Table/TablePagination";
import { TableSearch } from "~/components/Table/TableSearch";
import { UniversalTable } from "~/components/Table/UniversalTable";
import {
  getInitialTableState,
  useUniversalTable,
} from "~/components/Table/useUniversalTable";
import TextLink from "~/components/TextLink";
import { useSyncObjectToSearchParams } from "~/hooks/useSyncObjectToSearchParams";
import { ChevronDownIcon, ClockIcon } from "~/icons";
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
import { formatIntervalShort } from "~/utils/format";

import { LOOKBACK_OPTIONS } from "./constants";
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
  STATUS_COLOR_SCHEMES,
} from "./filters";
import { MaintainedObjectListItem } from "./queries";

const PAGE_SIZE = 25;

/** Compact label for the Live tag, mirroring the selected lookback window. */
const LOOKBACK_SHORT_LABELS: Record<string, string> = {
  "5": "5m",
  "15": "15m",
  "30": "30m",
  "60": "1h",
  "180": "3h",
  "360": "6h",
  "1440": "24h",
};

const WindowControl = ({
  timePeriodMinutes,
  setTimePeriodMinutes,
}: {
  timePeriodMinutes: number;
  setTimePeriodMinutes: (val: number) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const key = String(timePeriodMinutes);
  const currentLabel = LOOKBACK_OPTIONS[key] ?? "Custom window";
  const shortLabel = LOOKBACK_SHORT_LABELS[key];
  // Live is its own option separate from the historical windows below.
  const isLive = key === "1";

  return (
    <HStack spacing="2">
      <Menu>
        <Tooltip
          label="Lookback window applies to freshness only — it does not affect status."
          lineHeight={1.2}
          openDelay={300}
        >
          <MenuButton
            as={Button}
            variant="secondary"
            size="sm"
            leftIcon={<ClockIcon height="3" width="3" />}
            rightIcon={<ChevronDownIcon height="3" width="3" />}
          >
            {isLive ? (
              "Lookback Window"
            ) : (
              <>
                <Text as="span" color={colors.foreground.secondary}>
                  Window:{" "}
                </Text>
                {currentLabel}
              </>
            )}
          </MenuButton>
        </Tooltip>
        <MenuList>
          {Object.entries(LOOKBACK_OPTIONS).map(([value, label]) => (
            <MenuItem
              key={value}
              onClick={() => setTimePeriodMinutes(parseInt(value, 10))}
            >
              {label}
            </MenuItem>
          ))}
        </MenuList>
      </Menu>
      {isLive ? (
        <Tag size="sm" colorScheme="green" borderRadius="full" px="2">
          <Box
            w="1.5"
            h="1.5"
            borderRadius="full"
            bg={colors.accent.green}
            mr="1.5"
          />
          <TagLabel>Live</TagLabel>
        </Tag>
      ) : (
        <Tag size="md" borderRadius="md" px="3" py="1">
          <TagLabel>Window: {shortLabel ?? currentLabel}</TagLabel>
          <TagCloseButton
            aria-label="Reset window to Live"
            onClick={() => setTimePeriodMinutes(1)}
            ml="2"
          />
        </Tag>
      )}
    </HStack>
  );
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
const FRESH_MS = 1_000;

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
  if (!row.cluster) return "-";
  const path = absoluteClusterPath(regionSlug, row.cluster);
  return (
    <TextLink
      as={RouterLink}
      to={path}
      target="_blank"
      rel="noopener noreferrer"
      textStyle="text-ui-med"
      noOfLines={1}
      onClick={(e) => e.stopPropagation()}
    >
      {row.cluster.name}
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
  if (!row.lag) {
    return ready ? "-" : <Skeleton height="3" width="14" />;
  }
  const { value, ms } = row.lag;
  const color =
    ms >= OUTDATED_MS
      ? colors.accent.red
      : ms >= WARNING_MS
        ? colors.accent.orange
        : ms <= FRESH_MS
          ? colors.accent.green
          : colors.foreground.primary;
  return (
    <Text textStyle="text-ui-med" color={color}>
      {formatIntervalShort(value as Parameters<typeof formatIntervalShort>[0])}
    </Text>
  );
};

const StatusCell = ({
  row,
  ready,
}: {
  row: MaintainedObjectListItem;
  ready: boolean;
}) => {
  const { hydratedReplicas, totalReplicas } = row;
  if (totalReplicas === 0) {
    return ready ? "-" : <Skeleton height="5" width="20" borderRadius="full" />;
  }
  const bucket = bucketForHydration(hydratedReplicas, totalReplicas);
  if (!bucket) return "-";
  return (
    <StatusPill
      status={bucket}
      label={HYDRATION_LABELS[bucket]}
      colorScheme={STATUS_COLOR_SCHEMES[bucket]}
    />
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
  columnHelper.accessor((row) => row.lag?.ms ?? null, {
    id: "freshness",
    header: "Freshness (pMAX)",
    sortingFn: sortingFunctions.nullsLast,
    filterFn: freshnessFilterFn,
    cell: (info) => {
      const meta = info.table.options.meta as MaintainedObjectsTableMeta;
      return <FreshnessCell row={info.row.original} ready={meta.lagReady} />;
    },
    meta: {
      tooltip: "Maximum freshness observed over the lookback window.",
      renderFilter: (column) => <FreshnessFilterPanel column={column} />,
    },
  }),
  columnHelper.accessor(
    (row) =>
      row.totalReplicas === 0 ? null : row.hydratedReplicas / row.totalReplicas,
    {
      id: "status",
      header: "Status",
      sortingFn: sortingFunctions.nullsLast,
      filterFn: hydrationFilterFn,
      cell: (info) => {
        const meta = info.table.options.meta as MaintainedObjectsTableMeta;
        return (
          <StatusCell row={info.row.original} ready={meta.hydrationReady} />
        );
      },
      meta: {
        tooltip:
          "Object status derived from replica hydration across all clusters.",
        renderFilter: (column) => <HydrationFilterPanel column={column} />,
      },
    },
  ),
  columnHelper.accessor((row) => row.cluster?.name ?? null, {
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

export interface MaintainedObjectsProps {
  objects: MaintainedObjectListItem[];
  isLoading: boolean;
  lagReady: boolean;
  hydrationReady: boolean;
  lookbackMinutes: number;
  setLookbackMinutes: React.Dispatch<React.SetStateAction<number>>;
}

const MaintainedObjects = ({
  objects,
  isLoading,
  lagReady,
  hydrationReady,
  lookbackMinutes: timePeriodMinutes,
  setLookbackMinutes: setTimePeriodMinutes,
}: MaintainedObjectsProps) => {
  const location = useLocation();
  const navigate = useNavigate();
  const regionSlug = useRegionSlug();

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
      if (obj.cluster) names.add(obj.cluster.name);
    }
    return [...names].sort();
  }, [objects]);

  const table = useUniversalTable({
    data: objects ?? [],
    columns,
    state: { columnFilters, pagination },
    onColumnFiltersChange: setColumnFilters,
    onPaginationChange: setPagination,
    initialSorting: initialState.sorting ?? [{ id: "freshness", desc: false }],
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
          <PageHeading>Objects</PageHeading>
        </PageHeader>
        <Spinner data-testid="loading-spinner" />
      </MainContentContainer>
    );
  }

  const filteredCount = table.getFilteredRowModel().rows.length;
  const hasFilters = tableState.columnFilters.length > 0;
  const showEmpty = objects && filteredCount === 0;

  return (
    <MainContentContainer>
      <PageHeader variant="compact" sticky boxProps={{ pb: "4" }}>
        <PageHeading>Objects</PageHeading>
        <VStack alignItems="stretch" width="100%" spacing="3" mt="4">
          <HStack width="100%" flexWrap="wrap" gap="3">
            <TableSearch
              initialValue={initialState.globalFilter ?? ""}
              onValueChange={table.setGlobalFilter}
              placeholder="Search objects..."
            />
            <WindowControl
              timePeriodMinutes={timePeriodMinutes}
              setTimePeriodMinutes={setTimePeriodMinutes}
            />
          </HStack>
          {hasFilters && <FilterChips table={table} />}
        </VStack>
      </PageHeader>

      <VStack alignItems="stretch" width="100%" spacing="4">
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
            <Box overflowX="auto" width="100%">
              <UniversalTable
                table={table}
                variant="linkable"
                rowSx={{ cursor: "pointer" }}
                onRowClick={(row) => navigate(row.id)}
                data-testid="maintained-objects-table"
              />
            </Box>
            <TablePagination table={table} itemLabel="objects" />
          </>
        )}
      </VStack>
    </MainContentContainer>
  );
};

export { MaintainedObjects };
export default MaintainedObjects;
