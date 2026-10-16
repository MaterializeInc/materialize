// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Flex, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import { createColumnHelper, Table } from "@tanstack/react-table";
import React from "react";
import { Link as RouterLink } from "react-router-dom";

import { LagMap } from "~/api/materialize/cluster/materializationLag";
import { DatabaseObject } from "~/api/materialize/objects";
import { TablePagination } from "~/components/Table/TablePagination";
import { UniversalTable } from "~/components/Table/UniversalTable";
import { useUniversalTable } from "~/components/Table/useUniversalTable";
import TextLink from "~/components/TextLink";
import { formatLagInfoDetailed } from "~/platform/clusters/format";
import { MaterializeTheme } from "~/theme";
import { kebabToTitleCase, pluralize } from "~/util";
import { formatPercentage } from "~/utils/format";

import { useObjectLink } from "../useObjectLink";
import {
  ADHOC_ROW_ID,
  formatMultiple,
  isPooledRow,
  Measurement,
  NOTABLE_SHARE,
  ObjectCpuRow,
  ObjectSpread,
  objectSpread,
  SEVERE_SKEW_RATIO,
  SYSTEM_ROW_ID,
} from "./workerCpuModel";

const OBJECT_PAGE_SIZE = 10;

interface ObjectTableRow {
  cpu: ObjectCpuRow;
  name: string;
  /** The object's type, or what a pooled row holds. */
  detail: string;
  path: string | undefined;
  /** Where the queries row's statements are listed. */
  queryHistory?: QueryHistoryLinks;
  /** Of the selected worker's time over the measurement. */
  onWorker: number;
  /** Of the replica's CPU allocation over the measurement. */
  share: number;
  spread: ObjectSpread;
  /**
   * Its skew isn't worth flagging: the verdict found none, or the object has
   * too small a share of the workers' time.
   */
  isMuted: boolean;
  lag: string;
  isOutdated: boolean;
}

interface ObjectTableMeta {
  workerId: number;
  pickedId: string | undefined;
  onPick: (objectId: string) => void;
  /** Time that isn't any object's or query's, so the columns add up. */
  other: { onWorker: number; share: number };
}

const tableMeta = (table: Table<ObjectTableRow>) =>
  table.options.meta as ObjectTableMeta;

const ObjectNameCell = ({ row }: { row: ObjectTableRow }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack
      spacing={0}
      alignItems="flex-start"
      justifyContent="center"
      minHeight={11}
    >
      {row.path ? (
        <TextLink
          as={RouterLink}
          to={row.path}
          textStyle="text-ui-med"
          noOfLines={1}
          // Clicking the row picks the object, so following its link doesn't.
          onClick={(event: React.MouseEvent) => event.stopPropagation()}
        >
          {row.name}
        </TextLink>
      ) : (
        <Text textStyle="text-ui-med" noOfLines={1}>
          {row.name}
        </Text>
      )}
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        {row.detail}
      </Text>
      {row.queryHistory && (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          Query History:{" "}
          <QueryHistoryLink to={row.queryHistory.startedPath}>
            started in these minutes
          </QueryHistoryLink>
          {" · "}
          <QueryHistoryLink to={row.queryHistory.runningPath}>
            still running
          </QueryHistoryLink>
        </Text>
      )}
    </VStack>
  );
};

const QueryHistoryLink = ({
  to,
  children,
}: React.PropsWithChildren<{ to: string }>) => (
  <TextLink
    as={RouterLink}
    to={to}
    textStyle="text-small"
    // Clicking the row picks it, so following its link doesn't.
    onClick={(event: React.MouseEvent) => event.stopPropagation()}
  >
    {children}
  </TextLink>
);

const OtherDataflowsName = () => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack spacing={0} alignItems="flex-start">
      <Text textStyle="text-ui-med">Other dataflows</Text>
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        Dataflows that export no object
      </Text>
    </VStack>
  );
};

const SpreadCell = ({ row }: { row: ObjectTableRow }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { spread } = row;
  if (spread.kind === "even") {
    return <Text color={colors.foreground.secondary}>Even</Text>;
  }
  const isSevere = spread.kind === "allOn" || spread.ratio >= SEVERE_SKEW_RATIO;
  return (
    <Text
      textStyle={row.isMuted ? undefined : "text-ui-med"}
      color={
        row.isMuted
          ? colors.foreground.secondary
          : isSevere
            ? colors.accent.red
            : colors.accent.orange
      }
    >
      {spread.kind === "allOn"
        ? Number.isFinite(spread.ratio)
          ? `Nearly all on w${spread.workerId} · ${formatMultiple(spread.ratio)} the others`
          : `All on w${spread.workerId}`
        : `Mostly w${spread.workerId} · ${formatMultiple(spread.ratio)} the others`}
    </Text>
  );
};

const ShareBar = ({ share, isOther }: { share: number; isOther?: boolean }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack spacing={2}>
      <Box
        flex="1"
        minWidth="64px"
        height="8px"
        borderRadius="sm"
        background={colors.background.tertiary}
        position="relative"
        aria-hidden
      >
        <Box
          position="absolute"
          top={0}
          bottom={0}
          left={0}
          width={`${Math.max(0.5, Math.min(1, share) * 100)}%`}
          borderRadius="sm"
          background={
            isOther ? colors.foreground.tertiary : colors.accent.brightPurple
          }
        />
      </Box>
      <Text width="44px" textAlign="right">
        {formatPercentage(share, 0)}
      </Text>
    </HStack>
  );
};

/**
 * The row's time on the selected worker, as the button that picks the row
 * from the keyboard. A click anywhere in the row picks it too.
 */
const OnWorkerCell = ({
  row,
  meta,
}: {
  row: ObjectTableRow;
  meta: ObjectTableMeta;
}) => {
  const { shadows } = useTheme<MaterializeTheme>();
  const value = `${formatPercentage(row.onWorker, 0)} of w${meta.workerId}`;
  return (
    <Box
      as="button"
      type="button"
      aria-pressed={meta.pickedId === row.cpu.objectId}
      aria-label={`${value}. Show ${row.name} on the bars.`}
      onClick={(event: React.MouseEvent) => {
        event.stopPropagation();
        meta.onPick(row.cpu.objectId);
      }}
      borderRadius="sm"
      _focusVisible={{ outline: "none", boxShadow: shadows.input.focus }}
    >
      {value}
    </Box>
  );
};

const LagCell = ({ row }: { row: ObjectTableRow }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Text
      textStyle={row.isOutdated ? "text-ui-med" : undefined}
      color={row.isOutdated ? colors.accent.red : undefined}
      whiteSpace="nowrap"
    >
      {row.lag}
    </Text>
  );
};

const columnHelper = createColumnHelper<ObjectTableRow>();

const columns = [
  columnHelper.accessor("name", {
    id: "object",
    header: "Object",
    sortingFn: "alphanumeric",
    cell: ({ row }) => <ObjectNameCell row={row.original} />,
    footer: () => <OtherDataflowsName />,
    meta: { cellProps: { maxWidth: "260px" } },
  }),
  columnHelper.accessor("onWorker", {
    id: "onWorker",
    header: ({ table }) => `Time on w${tableMeta(table).workerId}`,
    cell: (info) => (
      <OnWorkerCell row={info.row.original} meta={tableMeta(info.table)} />
    ),
    footer: ({ table }) =>
      `${formatPercentage(tableMeta(table).other.onWorker, 0)} of w${tableMeta(table).workerId}`,
    meta: {
      tooltip:
        "How much of the selected worker's time went to the object over the measured span.",
      cellProps: { whiteSpace: "nowrap" },
    },
  }),
  columnHelper.accessor("share", {
    id: "share",
    header: "Share of replica CPU",
    cell: (info) => <ShareBar share={info.getValue()} />,
    footer: ({ table }) => (
      <ShareBar share={tableMeta(table).other.share} isOther />
    ),
    meta: {
      tooltip:
        "The object's time on all workers as a share of the replica's CPU allocation over the measured span, the unit of the CPU chart.",
      minWidth: "160px",
    },
  }),
  columnHelper.accessor((row) => row.cpu.ratio, {
    id: "spread",
    header: "Spread across workers",
    cell: (info) => <SpreadCell row={info.row.original} />,
    meta: {
      tooltip:
        "Whether the object's work is split evenly across the workers, or leans on one: how many times the other workers' average time its busiest worker spent on it.",
    },
  }),
  columnHelper.accessor("lag", {
    id: "lag",
    header: "Behind its inputs",
    enableSorting: false,
    cell: (info) => <LagCell row={info.row.original} />,
    meta: {
      tooltip:
        "How far the object's results trailed its inputs when it was measured. Skew on an object's worker shows up here first.",
    },
  }),
];

export interface ObjectCpuTableProps {
  measurement: Measurement;
  /** The worker whose time ranks the objects. */
  workerId: number;
  pickedId: string | undefined;
  onPick: (objectId: string) => void;
  /** The replica's CPU allocation over the measurement, in nanoseconds. */
  allocationNs: number;
  lagMap: LagMap | undefined;
  objectName: (objectId: string) => string;
  objectsById: Map<string, DatabaseObject>;
  /** Whether the verdict found skew, which makes a leaning object worth flagging. */
  isSkewed: boolean;
  queryHistory: QueryHistoryLinks;
}

/** Query History filtered to the statements behind the queries row. */
export interface QueryHistoryLinks {
  /** Those that began around the measured span. */
  startedPath: string;
  /** Those still running, including any that began well before the span. */
  runningPath: string;
}

/**
 * The objects behind a measurement's worker time, ranked by their time on one
 * worker. Picking a row shows its part of each worker's bar.
 */
export const ObjectCpuTable = ({
  measurement,
  workerId,
  pickedId,
  onPick,
  allocationNs,
  lagMap,
  objectName,
  objectsById,
  isSkewed,
  queryHistory,
}: ObjectCpuTableProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const objectLink = useObjectLink();
  const { rows, workerTotalsNs } = measurement;
  const workerNs = workerTotalsNs[workerId] ?? 0;
  const workersNs = workerTotalsNs.reduce((sum, ns) => sum + ns, 0);
  const data = React.useMemo(
    () =>
      rows.map((cpu): ObjectTableRow => {
        const object = objectsById.get(cpu.objectId);
        const lagInfo = lagMap?.get(cpu.objectId);
        const { dataflowCount } = cpu;
        return {
          cpu,
          name: objectName(cpu.objectId),
          detail:
            cpu.objectId === ADHOC_ROW_ID
              ? `${dataflowCount} ${pluralize(dataflowCount, "dataflow", "dataflows")} for sessions' statements`
              : cpu.objectId === SYSTEM_ROW_ID
                ? "Introspection and metrics, on every replica"
                : object
                  ? kebabToTitleCase(object.objectType)
                  : "",
          path:
            object &&
            objectLink({
              id: object.id,
              name: object.name,
              databaseName: object.databaseName,
              schemaName: object.schemaName,
              type: object.objectType,
            }),
          ...(cpu.objectId === ADHOC_ROW_ID && { queryHistory }),
          onWorker: workerNs > 0 ? cpu.perWorkerNs[workerId] / workerNs : 0,
          share: allocationNs > 0 ? cpu.totalNs / allocationNs : 0,
          spread: objectSpread(cpu),
          isMuted:
            !isSkewed ||
            workersNs <= 0 ||
            cpu.totalNs / workersNs < NOTABLE_SHARE,
          lag: isPooledRow(cpu.objectId) ? "-" : formatLagInfoDetailed(lagInfo),
          isOutdated: Boolean(lagInfo?.isOutdated),
        };
      }),
    [
      rows,
      objectsById,
      objectName,
      objectLink,
      lagMap,
      workerNs,
      workerId,
      allocationNs,
      workersNs,
      isSkewed,
      queryHistory,
    ],
  );
  const objectsOnWorkerNs = rows.reduce(
    (sum, row) => sum + row.perWorkerNs[workerId],
    0,
  );
  const objectsNs = rows.reduce((sum, row) => sum + row.totalNs, 0);
  const table = useUniversalTable({
    data,
    columns,
    getRowId: (row) => row.cpu.objectId,
    pageSize: OBJECT_PAGE_SIZE,
    initialSorting: [{ id: "onWorker", desc: true }],
    // One worker's work can't be uneven.
    state: { columnVisibility: { spread: workerTotalsNs.length > 1 } },
    meta: {
      workerId,
      pickedId,
      onPick,
      other: {
        onWorker:
          workerNs > 0
            ? Math.max(0, workerNs - objectsOnWorkerNs) / workerNs
            : 0,
        share:
          allocationNs > 0
            ? Math.max(0, workersNs - objectsNs) / allocationNs
            : 0,
      },
    } satisfies ObjectTableMeta,
  });
  return (
    <VStack spacing={3} alignItems="stretch">
      <Box overflowX="auto">
        <UniversalTable
          table={table}
          variant="standalone"
          onRowClick={(row) => onPick(row.cpu.objectId)}
          getRowClassName={(row) =>
            row.original.cpu.objectId === pickedId ? "picked" : undefined
          }
          rowSx={{ "&.picked td": { background: colors.background.accent } }}
          footerSx={{ td: { color: colors.foreground.secondary } }}
        />
      </Box>
      {rows.length > OBJECT_PAGE_SIZE && (
        <Flex justifyContent="flex-end">
          <TablePagination table={table} itemLabel="objects" />
        </Flex>
      )}
      {rows.length === 0 && (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          No object did measurable work in this span.
        </Text>
      )}
    </VStack>
  );
};
