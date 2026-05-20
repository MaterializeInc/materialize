// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Badge,
  Box,
  Flex,
  Text,
  Tooltip,
  useColorModeValue,
  useTheme,
} from "@chakra-ui/react";
import { interpolateRgb } from "d3-interpolate";
import React from "react";

import { MaterializeTheme } from "~/theme";

import { HeatmapRow } from "./workerSkewPivot";

export type SortMode = "skew" | "cpu";

export interface WorkerSkewHeatmapProps<T extends HeatmapRow> {
  rows: T[];
  numWorkers: number;
  globalWorkerTotals: number[];
  sortMode: SortMode;
  /** Header text for the row-label column. Defaults to "Dataflow". */
  rowLabelHeader?: string;
  onRowClick?: (row: T) => void;
}

const ROW_LABEL_WIDTH = 280;
const CELL_WIDTH = 28;
const CELL_HEIGHT = 24;
const LEGEND_BAR_WIDTH = 120;

/**
 * Heat gradient stops. Conventional chart colors (cold → warm → hot). The
 * `low` stop is theme-aware so a "no skew" cell fades into the drawer
 * background in both light and dark mode; `mid` is also nudged warmer in
 * light mode to keep contrast against the pale base. Intentionally not
 * pulled from the brand theme — accent tokens don't carry cold/warm/hot
 * semantics, and the gradient also has to be colorblind-safe (amber→red
 * works for protan/deuteran viewers; green→red would not).
 */
const HEAT_STOP_HIGH = "#ef4444"; // red — works in both light and dark
const HEAT_STOP_LOW_LIGHT = "#f1f5f9";
const HEAT_STOP_LOW_DARK = "#1e293b";
const HEAT_STOP_MID_LIGHT = "#fb923c";
const HEAT_STOP_MID_DARK = "#f59e0b";

const fmtHours = (ns: number) => `${(ns / 3.6e12).toFixed(1)}h`;
const fmtSkew = (s: number) => `${s.toFixed(1)}x`;
const fmtRatio = (r: number) => `${r.toFixed(2)}×`;

interface HeatStops {
  low: string;
  mid: string;
  high: string;
}

const useHeatScale = (): {
  stops: HeatStops;
  heat: (value: number, min: number, max: number) => string;
} => {
  const low = useColorModeValue(HEAT_STOP_LOW_LIGHT, HEAT_STOP_LOW_DARK);
  const mid = useColorModeValue(HEAT_STOP_MID_LIGHT, HEAT_STOP_MID_DARK);
  const lowToMid = interpolateRgb(low, mid);
  const midToHigh = interpolateRgb(mid, HEAT_STOP_HIGH);
  const heat = (value: number, min: number, max: number) => {
    if (max === min) return low;
    const t = Math.max(0, Math.min(1, (value - min) / (max - min)));
    return t <= 0.5 ? lowToMid(t * 2) : midToHigh((t - 0.5) * 2);
  };
  return { stops: { low, mid, high: HEAT_STOP_HIGH }, heat };
};

export const WorkerSkewHeatmap = <T extends HeatmapRow>({
  rows,
  numWorkers,
  globalWorkerTotals,
  sortMode,
  rowLabelHeader = "Dataflow",
  onRowClick,
}: WorkerSkewHeatmapProps<T>) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { heat, stops } = useHeatScale();

  // Sort each render — n is small (< 100 rows), so memoizing this isn't worth
  // the dependency tracking it forces on callers.
  const sorted = [...rows].sort((a, b) =>
    sortMode === "skew" ? b.skew - a.skew : b.total - a.total,
  );

  const globalMin = Math.min(...globalWorkerTotals);
  const globalMax = Math.max(...globalWorkerTotals);

  const gridTemplate = `${ROW_LABEL_WIDTH}px repeat(${numWorkers}, ${CELL_WIDTH}px)`;

  return (
    <Flex direction="column" gap={3}>
      <HeatmapDescription numWorkers={numWorkers} stops={stops} />
      <Box
        overflowX="auto"
        overflowY="auto"
        maxH="calc(100vh - 260px)"
        border={`1px solid ${colors.border.primary}`}
        borderRadius="md"
      >
        <Box display="grid" gridTemplateColumns={gridTemplate}>
          {/* Header row */}
          <Box
            position="sticky"
            top={0}
            left={0}
            zIndex={3}
            bg={colors.background.primary}
            borderBottom={`2px solid ${colors.border.primary}`}
            borderRight={`2px solid ${colors.border.primary}`}
            py={2}
            px={3}
            textStyle="text-small-heavy"
            color={colors.foreground.secondary}
          >
            {rowLabelHeader}
          </Box>
          {Array.from({ length: numWorkers }, (_, w) => (
            <Box
              key={`h-${w}`}
              position="sticky"
              top={0}
              zIndex={2}
              bg={colors.background.primary}
              borderBottom={`2px solid ${colors.border.primary}`}
              textAlign="center"
              textStyle="text-small"
              color={colors.foreground.secondary}
              py={2}
            >
              {w}
            </Box>
          ))}

          {/* Data rows */}
          {sorted.map((row) => (
            <HeatmapDataRow
              key={row.id}
              row={row}
              numWorkers={numWorkers}
              sortMode={sortMode}
              heat={heat}
              onClick={onRowClick}
            />
          ))}

          {/* Footer row: cluster-wide per-worker totals */}
          <Box
            position="sticky"
            left={0}
            zIndex={1}
            bg={colors.background.primary}
            borderTop={`2px solid ${colors.border.primary}`}
            borderRight={`2px solid ${colors.border.primary}`}
            py={2}
            px={3}
            textStyle="text-small-heavy"
          >
            All dataflows
          </Box>
          {globalWorkerTotals.map((v, w) => (
            <Tooltip
              key={`g-${w}`}
              hasArrow
              placement="top"
              label={
                <Box>
                  <Text textStyle="text-small-heavy">
                    Worker {w} — cluster total
                  </Text>
                  <Text textStyle="text-small">{fmtHours(v)} CPU</Text>
                </Box>
              }
            >
              <Box
                height={`${CELL_HEIGHT}px`}
                borderTop={`2px solid ${colors.border.primary}`}
                backgroundColor={heat(v, globalMin, globalMax)}
              />
            </Tooltip>
          ))}
        </Box>
      </Box>
    </Flex>
  );
};

interface HeatmapDescriptionProps {
  numWorkers: number;
  stops: HeatStops;
}

const HeatmapDescription = ({ numWorkers, stops }: HeatmapDescriptionProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Flex justify="space-between" align="flex-end" gap={6} wrap="wrap">
      <Box maxW="640px">
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          CPU distribution across all {numWorkers} workers, since the replica
          started.{" "}
          <Text as="span" textStyle="text-small-heavy">
            Horizontal patterns
          </Text>{" "}
          reveal object-level skew (often a bad <code>GROUP BY</code> key).{" "}
          <Text as="span" textStyle="text-small-heavy">
            Vertical columns
          </Text>{" "}
          reveal worker-level issues (a noisy neighbor).
        </Text>
      </Box>
      <Flex
        align="center"
        gap={2}
        textStyle="text-small"
        color={colors.foreground.secondary}
      >
        <Text>Low CPU</Text>
        <Box
          width={`${LEGEND_BAR_WIDTH}px`}
          height="10px"
          borderRadius="sm"
          backgroundImage={`linear-gradient(90deg, ${stops.low}, ${stops.mid}, ${stops.high})`}
        />
        <Text>Max CPU (per row)</Text>
      </Flex>
    </Flex>
  );
};

interface HeatmapDataRowProps<T extends HeatmapRow> {
  row: T;
  numWorkers: number;
  sortMode: SortMode;
  heat: (value: number, min: number, max: number) => string;
  onClick?: (row: T) => void;
}

const HeatmapDataRow = <T extends HeatmapRow>({
  row,
  numWorkers,
  sortMode,
  heat,
  onClick,
}: HeatmapDataRowProps<T>) => {
  const { colors } = useTheme<MaterializeTheme>();
  const clickable = Boolean(onClick) && row.clickable !== false;

  return (
    <>
      <Box
        position="sticky"
        left={0}
        zIndex={1}
        bg={colors.background.primary}
        borderBottom={`1px solid ${colors.border.secondary}`}
        borderRight={`2px solid ${colors.border.primary}`}
        py={1.5}
        px={3}
        display="flex"
        alignItems="center"
        justifyContent="space-between"
        gap={2}
        cursor={clickable ? "pointer" : "default"}
        _hover={
          clickable ? { background: colors.background.secondary } : undefined
        }
        onClick={clickable ? () => onClick?.(row) : undefined}
      >
        <Box minW={0} flex="1">
          <Text textStyle="text-small" noOfLines={1} title={row.label}>
            {row.label}
          </Text>
          {row.subLabel && (
            <Text
              textStyle="text-small"
              color={colors.foreground.secondary}
              noOfLines={1}
            >
              {row.subLabel}
            </Text>
          )}
        </Box>
        <Badge
          textStyle="text-small-heavy"
          colorScheme={sortMode === "skew" ? "red" : "gray"}
        >
          {sortMode === "skew" ? fmtSkew(row.skew) : fmtHours(row.total)}
        </Badge>
      </Box>
      {Array.from({ length: numWorkers }, (_, w) => {
        const v = row.workers[w] ?? 0;
        const pct = row.total > 0 ? (v / row.total) * 100 : 0;
        const ratioToAvg = row.avg > 0 ? v / row.avg : 0;
        return (
          <Tooltip
            key={`c-${row.id}-${w}`}
            hasArrow
            placement="top"
            label={
              <Box>
                <Text textStyle="text-small-heavy">{row.label}</Text>
                <Text textStyle="text-small">Worker {w}</Text>
                <Text textStyle="text-small">
                  {fmtHours(v)} ({pct.toFixed(1)}% of row)
                </Text>
                <Text textStyle="text-small">
                  vs average: {fmtRatio(ratioToAvg)}
                </Text>
                <Text textStyle="text-small">
                  Row skew (max÷min): {fmtSkew(row.skew)}
                </Text>
              </Box>
            }
          >
            <Box
              height={`${CELL_HEIGHT}px`}
              borderBottom={`1px solid ${colors.border.secondary}`}
              backgroundColor={heat(v, row.min, row.max)}
            />
          </Tooltip>
        );
      })}
    </>
  );
};
