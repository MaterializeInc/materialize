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
  Select,
  Text,
  useLatestRef,
  useTheme,
} from "@chakra-ui/react";
import * as Plot from "@observablehq/plot";
import React, { useEffect, useMemo, useRef, useState } from "react";

import { MaterializeTheme } from "~/theme";

import { COUNT_OF_ROWS, selectWidth } from "./cellLabels";
import { renderPieChart } from "./pieChart";
import { ChartMark, ChartSpec } from "./store";
import { useElementWidth } from "./useElementWidth";
import { WorksheetRow } from "./worksheetRuntime";

const CHART_HEIGHT_PX = 220;

type MarkOptions = Plot.BarYOptions &
  Plot.LineYOptions &
  Plot.AreaYOptions &
  Plot.DotOptions;

const PLOT_MARKS: Record<
  Exclude<ChartMark, "pie">,
  (data: Plot.Data, options: MarkOptions) => Plot.Markish
> = {
  bar: Plot.barY,
  line: Plot.lineY,
  area: Plot.areaY,
  dot: Plot.dot,
};

const CHART_MARKS: ChartMark[] = [
  ...(Object.keys(PLOT_MARKS) as ChartMark[]),
  "pie",
];

/** One row per distinct value of `column`, with the number of rows that had it. */
function countRowsBy(rows: WorksheetRow[], column: string): WorksheetRow[] {
  const counts = new Map<unknown, number>();
  for (const row of rows) {
    counts.set(row[column], (counts.get(row[column]) ?? 0) + 1);
  }
  return Array.from(counts, ([value, count]) => ({
    [column]: value,
    [COUNT_OF_ROWS]: count,
  }));
}

/** Orders rows by one column: numbers and dates by value, text naturally. */
function sortByColumn(rows: WorksheetRow[], column: string) {
  const collator = new Intl.Collator(undefined, { numeric: true });
  const sortKey = (value: unknown) =>
    value instanceof Date ? value.getTime() : value;
  return [...rows].sort((a, b) => {
    const x = sortKey(a[column]);
    const y = sortKey(b[column]);
    if (typeof x === "number" && typeof y === "number") return x - y;
    return collator.compare(String(x), String(y));
  });
}

/** Picks the first column for x and the first numeric column for y. */
function pickAxes(
  rows: WorksheetRow[],
): Pick<ChartSpec, "x" | "y"> | undefined {
  const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
  if (columns.length === 0) return undefined;
  const numeric = columns.find((c) => typeof rows[0][c] === "number");
  // Without a numeric column, a count of rows per x is the only meaningful y.
  return { x: columns[0], y: numeric ?? COUNT_OF_ROWS };
}

export interface ChartViewProps {
  chart: ChartSpec;
  /** The rows to plot, or undefined while there are none to show. */
  rows: WorksheetRow[] | undefined;
  /** All the source's rows, when `rows` holds only the first of them. */
  totalRows?: number;
  onChange: (update: Partial<ChartSpec>) => void;
  /** Hides the plot and messages, keeping the controls. */
  collapsed?: boolean;
  /** False on a dashboard, which shows only the plot. */
  showControls?: boolean;
  /** Shown while `rows` is undefined. */
  unavailableMessage?: string;
  /** Controls placed before the chart's own, such as a source picker. */
  leading?: React.ReactNode;
  /** Controls placed after the row count, such as a cell menu. */
  trailing?: React.ReactNode;
}

/**
 * A chart of `rows`: the mark and axis pickers and the plot. It picks axes
 * once rows arrive if none are chosen, so `rows` must belong to the chart's
 * current source.
 */
export const ChartView = ({
  chart,
  rows,
  totalRows,
  onChange,
  collapsed = false,
  showControls = true,
  unavailableMessage,
  leading,
  trailing,
}: ChartViewProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const plotRef = useRef<HTMLDivElement>(null);
  const width = useElementWidth(plotRef);
  const chartRef = useLatestRef(chart);
  const onChangeRef = useLatestRef(onChange);

  useEffect(() => {
    const current = chartRef.current;
    if (!rows || (current.x && current.y)) return;
    const axes = pickAxes(rows);
    if (axes) {
      onChangeRef.current({
        x: current.x || axes.x,
        y: current.y || axes.y,
      });
    }
  }, [rows, chartRef, onChangeRef]);

  const columns = useMemo(
    () => (rows && rows.length > 0 ? Object.keys(rows[0]) : []),
    [rows],
  );
  const isPie = chart.mark === "pie";
  const isCount = chart.y === COUNT_OF_ROWS;
  const hasAxes =
    columns.includes(chart.x) && (isCount || columns.includes(chart.y));
  const canPlot =
    !collapsed && rows !== undefined && rows.length > 0 && hasAxes && width > 0;

  const setChart = (update: Partial<ChartSpec>) => onChange(update);

  const [drawError, setDrawError] = useState<string>();

  useEffect(() => {
    const container = plotRef.current;
    if (!container || !canPlot || !rows) return;
    let figure: Element;
    try {
      figure = drawChart();
    } catch (e) {
      // A chart that cannot be drawn must not take the worksheet down.
      setDrawError(e instanceof Error ? e.message : String(e));
      container.replaceChildren();
      return;
    }
    setDrawError(undefined);
    container.replaceChildren(figure);
    return () => figure.remove();

    function drawChart(): Element {
      if (!rows) throw new Error("No rows");
      if (isPie) {
        const slices = isCount ? countRowsBy(rows, chart.x) : rows;
        return renderPieChart(slices, {
          label: chart.x,
          value: isCount ? COUNT_OF_ROWS : chart.y,
          width,
          height: CHART_HEIGHT_PX,
          textColor: colors.foreground.primary,
        });
      } else {
        // A column name maps to a color scale. Without one, every mark takes
        // the accent color.
        const color = columns.includes(chart.color)
          ? chart.color
          : colors.accent.purple;
        const stroked = chart.mark === "line" || chart.mark === "dot";
        // SUBSCRIBE rows arrive in no particular order, and a line drawn
        // through them in arrival order zigzags. NOTE: sorting here rather than
        // with the mark's `sort` option, which crashes areaY on an ordinal x
        // in Plot 0.6.17.
        // groupX orders its output by x itself.
        const ordered =
          !isCount && (chart.mark === "line" || chart.mark === "area")
            ? sortByColumn(rows, chart.x)
            : rows;
        const channels = {
          x: chart.x,
          ...(stroked ? { stroke: color } : { fill: color }),
          tip: true,
        };
        return Plot.plot({
          width,
          height: CHART_HEIGHT_PX,
          marginLeft: 50,
          style: {
            background: "transparent",
            color: colors.foreground.primary,
            fontSize: "11px",
            // Plot's tooltips fill with this variable, which defaults to white.
            ["--plot-background" as string]: colors.background.primary,
          },
          color: { legend: columns.includes(chart.color) },
          // Plot labels a count "Frequency"; match the y picker instead.
          ...(isCount ? { y: { label: COUNT_OF_ROWS } } : {}),
          marks: [
            PLOT_MARKS[chart.mark as Exclude<ChartMark, "pie">](
              ordered,
              isCount
                ? Plot.groupX({ y: "count" }, channels)
                : { ...channels, y: chart.y },
            ),
            Plot.ruleY([0]),
          ],
        });
      }
    }
  }, [
    canPlot,
    rows,
    columns,
    chart,
    isPie,
    isCount,
    width,
    colors.foreground.primary,
    colors.background.primary,
    colors.accent.purple,
  ]);

  const message = drawError
    ? `This chart can't be drawn: ${drawError}`
    : !rows
      ? unavailableMessage
      : rows.length === 0
        ? "No rows to plot"
        : !hasAxes
          ? "Choose the columns to plot"
          : undefined;

  const columnSelect = (
    label: string,
    value: string,
    onSelect: (v: string) => void,
    { allowNone = false, allowCount = false } = {},
  ) => (
    <HStack spacing="1">
      <Text
        as="span"
        textStyle="text-small"
        color={colors.foreground.secondary}
      >
        {label}
      </Text>
      <Select
        size="xs"
        flexShrink={0}
        width={selectWidth([
          value,
          ...columns,
          allowNone ? "none" : "",
          allowCount ? COUNT_OF_ROWS : "",
        ])}
        aria-label={label}
        value={value}
        onChange={(e) => onSelect(e.target.value)}
      >
        {allowNone && <option value="">none</option>}
        {allowCount && <option value={COUNT_OF_ROWS}>{COUNT_OF_ROWS}</option>}
        {!allowNone && !columns.includes(value) && value !== COUNT_OF_ROWS && (
          <option value={value}>{value || "–"}</option>
        )}
        {columns.map((c) => (
          <option key={c} value={c}>
            {c}
          </option>
        ))}
      </Select>
    </HStack>
  );

  const truncated =
    rows && totalRows !== undefined && totalRows > rows.length
      ? ` of ${totalRows.toLocaleString()}`
      : "";

  return (
    <Box minW="0">
      <HStack
        spacing="2"
        pr="1"
        height="7"
        overflowX="auto"
        overflowY="hidden"
        display={showControls ? "flex" : "none"}
      >
        {leading}
        <Select
          size="xs"
          flexShrink={0}
          width={selectWidth(CHART_MARKS)}
          aria-label="Mark"
          title="Chart type"
          value={chart.mark}
          onChange={(e) => setChart({ mark: e.target.value as ChartMark })}
        >
          {CHART_MARKS.map((m) => (
            <option key={m} value={m}>
              {m}
            </option>
          ))}
        </Select>
        {columnSelect(isPie ? "label" : "x", chart.x, (x) => setChart({ x }))}
        {columnSelect(isPie ? "value" : "y", chart.y, (y) => setChart({ y }), {
          allowCount: true,
        })}
        {/* Pie slices are always colored by label. */}
        {!isPie &&
          columnSelect("color", chart.color, (color) => setChart({ color }), {
            allowNone: true,
          })}
        <Box flex="1" />
        {rows && rows.length > 0 && (
          <Text
            textStyle="text-small"
            color={colors.foreground.secondary}
            whiteSpace="nowrap"
            title={
              truncated
                ? `Charts plot the first ${rows.length.toLocaleString()} rows`
                : undefined
            }
          >
            {rows.length.toLocaleString()}
            {truncated} {rows.length === 1 && !truncated ? "row" : "rows"}
          </Text>
        )}
        {trailing}
      </HStack>
      {!collapsed && message && (
        <Text
          px="2"
          pb="2"
          textStyle="text-small"
          color={colors.foreground.tertiary}
        >
          {message}
        </Text>
      )}
      {/* Always mounted, so its width is measured even while collapsed. */}
      <Box
        px="2"
        pb={canPlot ? "1" : "0"}
        display={collapsed ? "none" : "block"}
      >
        <Box
          ref={plotRef}
          minH={canPlot ? `${CHART_HEIGHT_PX}px` : undefined}
        />
      </Box>
    </Box>
  );
};
