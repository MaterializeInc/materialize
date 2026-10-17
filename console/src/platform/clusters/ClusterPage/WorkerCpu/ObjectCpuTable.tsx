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
  ButtonGroup,
  Flex,
  HStack,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import {
  createColumnHelper,
  functionalUpdate,
  SortingState,
  Table,
} from "@tanstack/react-table";
import React from "react";
import { Link as RouterLink } from "react-router-dom";

import { DatabaseObject } from "~/api/materialize/objects";
import { TablePagination } from "~/components/Table/TablePagination";
import { UniversalTable } from "~/components/Table/UniversalTable";
import { useUniversalTable } from "~/components/Table/useUniversalTable";
import TextLink from "~/components/TextLink";
import { MaterializeTheme } from "~/theme";
import { pluralize } from "~/util";
import { formatDate, TIME_FORMAT } from "~/utils/dateFormat";
import { formatDurationExact, formatPercentage } from "~/utils/format";

import { useObjectLink } from "../useObjectLink";
import { TrackBar } from "./BusiestWorkers";
import {
  ADHOC_ROW_ID,
  EVEN_RATIO,
  NOTABLE_SHARE,
  ObjectCpuRow,
  SKEW_RATIO,
  WorkerReplica,
} from "./workerCpuModel";
import { BAR_CEILING_RATIO, useWorkerBarColor } from "./workerCpuStyles";

const OBJECT_PAGE_SIZE = 10;
const WORKER_BARS_WIDTH = 240;
const WORKER_BARS_HEIGHT = 28;
const PROCESS_GAP = 4;

/** Which span the table measures. "auto" picks the window once it exists. */
export type ObjectMode = "auto" | "window" | "lifetime";

interface ObjectTableRow {
  cpu: ObjectCpuRow;
  name: string;
  path: string | undefined;
  /** Of the replica's allocation, or of all dataflow time without one. */
  share: number;
  /** Too small a share of the dataflow time to flag its skew. */
  isMinor: boolean;
}

interface ObjectTableMeta {
  replica: WorkerReplica;
  workerCount: number;
  /** Dataflows that export no user object, so the shares add up. */
  other: { share: number; totalNs: number };
}

const tableMeta = (table: Table<ObjectTableRow>) =>
  table.options.meta as ObjectTableMeta;

const EvenShareKey = () => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <svg width={18} height={8} aria-hidden>
      <line
        x1={0}
        x2={18}
        y1={4}
        y2={4}
        stroke={colors.foreground.secondary}
        strokeDasharray="3 2"
      />
    </svg>
  );
};

/**
 * One bar per worker for its time on the object, against a dashed line at an
 * even share, so even work reads as a flat row at the line.
 */
const WorkerBars = ({
  row,
  workerCount,
  workersPerProcess,
  isMinor,
}: {
  row: ObjectCpuRow;
  workerCount: number;
  workersPerProcess: number;
  isMinor: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const skewColor = useWorkerBarColor();
  const barColor = isMinor ? () => colors.foreground.tertiary : skewColor;
  const perProcess = Math.max(1, workersPerProcess);
  const processCount = Math.ceil(workerCount / perProcess);
  const slot =
    (WORKER_BARS_WIDTH - PROCESS_GAP * (processCount - 1)) / workerCount;
  // Touching bars of one height merge into a block, so wide slots keep a gap.
  const barWidth = slot >= 4 ? slot - 1 : slot;
  const yOf = (ratio: number) =>
    WORKER_BARS_HEIGHT *
    (1 - Math.min(ratio, BAR_CEILING_RATIO) / BAR_CEILING_RATIO);
  const evenY = yOf(1);
  return (
    <svg
      width={WORKER_BARS_WIDTH}
      height={WORKER_BARS_HEIGHT}
      role="img"
      aria-label={`Busiest worker, w${row.hottestWorkerId}, at ${row.ratio.toFixed(1)} times an even share`}
    >
      <line
        x1={0}
        x2={WORKER_BARS_WIDTH}
        y1={WORKER_BARS_HEIGHT - 0.5}
        y2={WORKER_BARS_HEIGHT - 0.5}
        stroke={colors.border.primary}
      />
      {row.perWorkerNs.map((ns, workerId) => {
        const ratio = row.averageNs > 0 ? ns / row.averageNs : 0;
        const y = yOf(ratio);
        return (
          <rect
            key={workerId}
            x={
              workerId * slot + Math.floor(workerId / perProcess) * PROCESS_GAP
            }
            y={y}
            width={barWidth}
            height={WORKER_BARS_HEIGHT - y}
            fill={barColor(ratio)}
          >
            <title>
              {`w${workerId}: ${formatDurationExact(ns / 1e6)}, ${ratio.toFixed(2)}× an even share`}
            </title>
          </rect>
        );
      })}
      <line
        x1={0}
        x2={WORKER_BARS_WIDTH}
        y1={evenY}
        y2={evenY}
        stroke={colors.foreground.secondary}
        strokeDasharray="3 2"
      />
    </svg>
  );
};

const ObjectNameCell = ({ row }: { row: ObjectTableRow }) => {
  const { colors } = useTheme<MaterializeTheme>();
  if (row.path) {
    return (
      <TextLink as={RouterLink} to={row.path} noOfLines={1}>
        {row.name}
      </TextLink>
    );
  }
  const { dataflowCount } = row.cpu;
  return (
    <Text noOfLines={1}>
      {row.name}
      {row.cpu.objectId === ADHOC_ROW_ID && (
        <Text as="span" color={colors.foreground.secondary}>
          {" "}
          · {dataflowCount} temporary{" "}
          {pluralize(dataflowCount, "dataflow", "dataflows")}
        </Text>
      )}
    </Text>
  );
};

const OtherDataflowsName = () => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Text noOfLines={1}>
      Other dataflows
      <Text as="span" color={colors.foreground.secondary}>
        {" "}
        · system and introspection
      </Text>
    </Text>
  );
};

const HottestWorkerCell = ({
  row,
  isMinor,
}: {
  row: ObjectCpuRow;
  isMinor: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const isEven = row.ratio < EVEN_RATIO;
  return (
    <Text
      color={
        isEven || isMinor
          ? colors.foreground.secondary
          : row.ratio >= SKEW_RATIO
            ? colors.accent.red
            : undefined
      }
    >
      {isEven
        ? `even, max ${row.ratio.toFixed(1)}×`
        : `w${row.hottestWorkerId} · ${row.ratio.toFixed(1)}×`}
    </Text>
  );
};

const ShareBar = ({ share, isOther }: { share: number; isOther?: boolean }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack spacing={2}>
      <TrackBar
        to={share}
        height="8px"
        color={isOther ? colors.foreground.tertiary : colors.accent.purple}
      />
      <Text width="40px" textAlign="right">
        {formatPercentage(share, 0)}
      </Text>
    </HStack>
  );
};

const columnHelper = createColumnHelper<ObjectTableRow>();

const columns = [
  columnHelper.accessor("name", {
    id: "object",
    header: "Object",
    sortingFn: "alphanumeric",
    cell: (info) => <ObjectNameCell row={info.row.original} />,
    footer: () => <OtherDataflowsName />,
    meta: { cellProps: { maxWidth: "220px" } },
  }),
  columnHelper.display({
    id: "workers",
    header: ({ table }) =>
      tableMeta(table).replica.processes > 1
        ? "Time per worker, by process"
        : "Time per worker",
    cell: ({ row, table }) => (
      <WorkerBars
        row={row.original.cpu}
        workerCount={tableMeta(table).workerCount}
        workersPerProcess={tableMeta(table).replica.workersPerProcess}
        isMinor={row.original.isMinor}
      />
    ),
    enableSorting: false,
  }),
  columnHelper.accessor((row) => row.cpu.ratio, {
    id: "hottest",
    header: "Hottest worker",
    cell: (info) => (
      <HottestWorkerCell
        row={info.row.original.cpu}
        isMinor={info.row.original.isMinor}
      />
    ),
    meta: {
      tooltip:
        "The worker that spent the longest on the object, as a multiple of an even share. 1× is even, and 2× is twice an even share.",
      cellProps: { whiteSpace: "nowrap" },
    },
  }),
  columnHelper.accessor((row) => row.cpu.extraNs, {
    id: "extra",
    header: "Extra time",
    cell: (info) =>
      info.row.original.cpu.ratio >= EVEN_RATIO
        ? `+${formatDurationExact(info.getValue() / 1e6)}`
        : null,
    meta: {
      tooltip:
        "How much longer the hottest worker spent on the object than an even share. The object's results wait for that worker.",
      isNumeric: true,
      cellProps: { whiteSpace: "nowrap" },
    },
  }),
  // Two headers over one value: a window has an allocation to measure
  // against, and a lifetime, whose dataflows started at different times,
  // doesn't.
  columnHelper.accessor("share", {
    id: "cpuShare",
    header: "% of replica CPU",
    cell: (info) => <ShareBar share={info.getValue()} />,
    footer: ({ table }) => (
      <ShareBar share={tableMeta(table).other.share} isOther />
    ),
    meta: {
      tooltip:
        "The object's CPU time as a share of the replica's CPU allocation over the same span, the unit of the CPU chart. Objects, other dataflows and work outside running dataflows add up to the replica's CPU.",
      minWidth: "160px",
    },
  }),
  columnHelper.accessor("share", {
    id: "dataflowShare",
    header: "Share of dataflow CPU",
    cell: (info) => <ShareBar share={info.getValue()} />,
    footer: ({ table }) => (
      <ShareBar share={tableMeta(table).other.share} isOther />
    ),
    meta: {
      tooltip:
        "The object's part of the CPU time workers spent running dataflows, including system dataflows and ad-hoc queries. CPU used outside dataflows, such as scheduling and networking, isn't counted, so this isn't a share of the replica's CPU.",
      minWidth: "160px",
    },
  }),
  columnHelper.accessor((row) => row.cpu.totalNs, {
    id: "cpuTime",
    header: "CPU time",
    cell: (info) => formatDurationExact(info.getValue() / 1e6),
    footer: ({ table }) =>
      formatDurationExact(tableMeta(table).other.totalNs / 1e6),
    meta: {
      tooltip:
        "How long workers spent running the object's dataflow, added up across all workers.",
      isNumeric: true,
      cellProps: { whiteSpace: "nowrap" },
    },
  }),
];

export interface ObjectCpuTableProps {
  rows: ObjectCpuRow[];
  /** The order until someone sorts by a column. */
  sortBy: "extra" | "total";
  mode: "window" | "lifetime";
  canMeasureWindow: boolean;
  sinceMs: number | undefined;
  onModeChange: (mode: ObjectMode) => void;
  /** All dataflow time in the same span, system and ad-hoc included. */
  dataflowTotalNs: number;
  /**
   * The replica's CPU allocation over a window, and its measured CPU then.
   * With it, shares are of the allocation. Without it, of `dataflowTotalNs`.
   */
  allocation:
    { ns: number; replicaCpuFraction: number | undefined } | undefined;
  replica: WorkerReplica;
  objectName: (objectId: string) => string;
  objectsById: Map<string, DatabaseObject>;
}

// Memoized because its rows change only with the per-object read, while the
// panel renders on every worker reading.
export const ObjectCpuTable = React.memo(
  ({
    rows,
    sortBy,
    mode,
    canMeasureWindow,
    sinceMs,
    onModeChange,
    dataflowTotalNs,
    allocation,
    replica,
    objectName,
    objectsById,
  }: ObjectCpuTableProps) => {
    const { colors } = useTheme<MaterializeTheme>();
    const objectLink = useObjectLink();
    const workerCount = replica.processes * replica.workersPerProcess;
    const basisNs = allocation?.ns ?? dataflowTotalNs;
    const data = React.useMemo(
      () =>
        rows.map((cpu): ObjectTableRow => {
          const object = objectsById.get(cpu.objectId);
          return {
            cpu,
            name: objectName(cpu.objectId),
            path:
              object &&
              objectLink({
                id: object.id,
                name: object.name,
                databaseName: object.databaseName,
                schemaName: object.schemaName,
                type: object.objectType,
              }),
            share: basisNs > 0 ? cpu.totalNs / basisNs : 0,
            isMinor:
              dataflowTotalNs <= 0 ||
              cpu.totalNs / dataflowTotalNs < NOTABLE_SHARE,
          };
        }),
      [rows, objectsById, objectName, objectLink, basisNs, dataflowTotalNs],
    );
    const otherNs = Math.max(
      0,
      dataflowTotalNs - rows.reduce((sum, row) => sum + row.totalNs, 0),
    );
    const [sorting, setSorting] = React.useState<SortingState>();
    const effectiveSorting = React.useMemo(
      () =>
        sorting ?? [
          { id: sortBy === "extra" ? "extra" : "cpuTime", desc: true },
        ],
      [sorting, sortBy],
    );
    const table = useUniversalTable({
      data,
      columns,
      getRowId: (row) => row.cpu.objectId,
      pageSize: OBJECT_PAGE_SIZE,
      state: {
        sorting: effectiveSorting,
        columnVisibility: {
          extra: data.some(
            (row) => !row.isMinor && row.cpu.ratio >= EVEN_RATIO,
          ),
          cpuShare: Boolean(allocation),
          dataflowShare: !allocation,
        },
      },
      onSortingChange: (updater) =>
        setSorting(functionalUpdate(updater, effectiveSorting)),
      meta: {
        replica,
        workerCount,
        other: {
          share: basisNs > 0 ? otherNs / basisNs : 0,
          totalNs: otherNs,
        },
      } satisfies ObjectTableMeta,
    });
    return (
      <VStack spacing={3} alignItems="stretch">
        <Flex
          justifyContent="space-between"
          alignItems="center"
          gap={3}
          flexWrap="wrap"
        >
          <Text textStyle="text-ui-med">Where the CPU went</Text>
          <ButtonGroup size="sm" isAttached variant="outline">
            <Button
              aria-pressed={mode === "window"}
              isDisabled={!canMeasureWindow}
              onClick={() => onModeChange("window")}
            >
              {sinceMs !== undefined
                ? `Since ${formatDate(new Date(sinceMs), TIME_FORMAT)}`
                : "Since you opened this"}
            </Button>
            <Button
              aria-pressed={mode === "lifetime"}
              onClick={() => onModeChange("lifetime")}
            >
              Since each dataflow started
            </Button>
          </ButtonGroup>
        </Flex>
        <Box overflowX="auto">
          <UniversalTable
            table={table}
            variant="standalone"
            footerSx={{ td: { color: colors.foreground.secondary } }}
          />
        </Box>
        {allocation?.replicaCpuFraction !== undefined && (
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            Work outside running dataflows took{" "}
            {formatPercentage(
              Math.max(
                0,
                allocation.replicaCpuFraction - dataflowTotalNs / allocation.ns,
              ),
              0,
            )}{" "}
            of the allocation over the same span, for{" "}
            {formatPercentage(allocation.replicaCpuFraction, 0)} of the
            replica&apos;s CPU in all. It includes dataflows that finished
            between readings, such as those for ad-hoc queries.
          </Text>
        )}
        <Flex
          justifyContent="space-between"
          alignItems="center"
          gap={3}
          flexWrap="wrap"
        >
          <HStack spacing={2} alignItems="center">
            <EvenShareKey />
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              An even share, 1×: the object&apos;s CPU time split across its{" "}
              {workerCount} workers. Each bar is one worker, and bars stop at{" "}
              {BAR_CEILING_RATIO}×.
            </Text>
          </HStack>
          {rows.length > OBJECT_PAGE_SIZE && (
            <TablePagination table={table} itemLabel="objects" />
          )}
        </Flex>
      </VStack>
    );
  },
);
