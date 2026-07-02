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
  Grid,
  HStack,
  Spinner,
  Text,
  Tooltip,
  useTheme,
} from "@chakra-ui/react";
import { AxisBottom, AxisLeft, AxisScale } from "@visx/axis";
import { curveMonotoneX } from "@visx/curve";
import { Group } from "@visx/group";
import ParentSize, {
  ParentSizeProvidedProps,
} from "@visx/responsive/lib/components/ParentSize";
import { scaleBand, scaleLinear, scaleOrdinal } from "@visx/scale";
import { BarStack, LinePath } from "@visx/shape";
import { useTooltip, useTooltipInPortal } from "@visx/tooltip";
import { parseISO } from "date-fns";
import { motion } from "framer-motion";
import React, { useMemo, useState } from "react";

import { CostBreakdownCluster, CostBreakdownDay } from "~/api/cloudGlobalApi";
import ErrorBox from "~/components/ErrorBox";
import { GraphTooltip } from "~/components/graphComponents";
import { MaterializeTheme } from "~/theme";
import { formatDateInUtc } from "~/utils/dateFormat";
import { formatCurrency } from "~/utils/format";
import {
  buildXYGraphLayoutProps,
  calculateTooltipPosition,
} from "~/utils/graph";

import { baseCellStyles } from "./constants";
import {
  accountDailyTotals,
  accountIdsByTotal,
  aggregateDays,
  clusterTotal,
  StackedDailyRow,
  stackedDailyRows,
} from "./dailyBreakdown";

const ALL_ACCOUNTS = "all";

const margin = { top: 10, right: 0, bottom: 36, left: 50 };
const legendHeightPx = 18;
const chartHeightPx = 245;
const TOOLTIP_DISMISS_TIMEOUT_MS = 300;
const SPARKLINE_STROKE_WIDTH = 2;

type AccountSpendBreakdownProps = {
  days: CostBreakdownDay[] | null;
  isLoading: boolean;
  isError: boolean;
  error: Error | null;
};

/**
 * Names aren't available yet (SAS-141/142), so accounts are labelled by their
 * `external_customer_id` UUID. The full id is very wide for a chip or a table
 * cell, so we show the first segment and keep the whole id in a tooltip.
 */
function shortId(accountId: string): string {
  return `${accountId.slice(0, 8)}…`;
}

/**
 * A cluster's region-qualified label, mirroring the "Spend between …" table:
 * "aws/us-east-1 / quickstart.r1", "… / Storage", "… / Egress". Compute rows
 * carry the replica grouping key; storage/egress rows have an empty key and a
 * `category` instead, so the two stay distinct. "Other" is a defensive
 * fallback for the unexpected case where neither is set.
 */
function clusterLabel(cluster: CostBreakdownCluster): string {
  const label = cluster.cluster_grouping_key || cluster.category || "Other";
  return `${cluster.region} / ${label}`;
}

// Cycled through, in order, to color account layers/legend/table swatches. Kept
// visually distinct; wraps if an org has more accounts than colors.
function useAccountColors(accountIds: string[]): Map<string, string> {
  const { colors } = useTheme<MaterializeTheme>();
  return useMemo(() => {
    const palette = [
      colors.accent.purple,
      colors.accent.green,
      colors.accent.orange,
      colors.accent.blue,
      colors.accent.brightPurple,
      colors.accent.red,
      colors.accent.darkGreen,
      colors.accent.darkYellow,
    ];
    return new Map(
      accountIds.map((id, ix) => [id, palette[ix % palette.length]]),
    );
  }, [accountIds, colors]);
}

function shortDate(dateString: string): string {
  return formatDateInUtc(parseISO(dateString), "M/d");
}

function longDate(dateString: string): string {
  return formatDateInUtc(parseISO(dateString), "M/d/yyyy");
}

type ChartTooltipData = { row: StackedDailyRow };

type AccountSpendChartGraphProps = ParentSizeProvidedProps & {
  rows: StackedDailyRow[];
  accountIds: string[];
  colorFor: Map<string, string>;
};

const AccountSpendChartGraph = ({
  rows,
  accountIds,
  colorFor,
  width,
  height,
}: AccountSpendChartGraphProps) => {
  const { colors, textStyles } = useTheme<MaterializeTheme>();
  const { svgProps, axisLeftProps, axisBottomProps, xScaleRange, yScaleRange } =
    useMemo(
      () => buildXYGraphLayoutProps({ width, height, margin }),
      [width, height],
    );
  const {
    tooltipOpen,
    tooltipLeft,
    tooltipTop,
    tooltipData,
    hideTooltip,
    showTooltip,
  } = useTooltip<ChartTooltipData>();
  const { containerRef, TooltipInPortal } = useTooltipInPortal({
    detectBounds: true,
    scroll: true,
  });
  const tooltipTimeoutRef = React.useRef<number>();

  const maxDailyTotal = useMemo(
    () =>
      rows.reduce((currentMax, row) => {
        const total = accountIds.reduce(
          (sum, id) => sum + (row[id] as number),
          0,
        );
        return Math.max(currentMax, total);
      }, 0),
    [rows, accountIds],
  );
  const barPadding = rows.length > 90 ? 0 : rows.length > 7 ? 0.25 : 0.15;
  const xScale = useMemo(
    () =>
      scaleBand<string>({
        domain: rows.map((row) => shortDate(row.startDate)),
        range: xScaleRange as [number, number],
        padding: barPadding,
      }),
    [rows, xScaleRange, barPadding],
  );
  const yScale = useMemo(
    () =>
      scaleLinear({
        domain: [0, Math.ceil(maxDailyTotal)],
        range: yScaleRange,
        nice: true,
      }),
    [maxDailyTotal, yScaleRange],
  );
  const colorScale = useMemo(
    () =>
      scaleOrdinal<string, string>({
        domain: accountIds,
        range: accountIds.map((id) => colorFor.get(id) ?? colors.accent.purple),
      }),
    [accountIds, colorFor, colors],
  );

  return (
    <Box position="relative">
      <svg data-testid="account-spend-chart" ref={containerRef} {...svgProps}>
        <Group top={margin.top}>
          <BarStack<StackedDailyRow, string>
            data={rows}
            keys={accountIds}
            x={(row) => shortDate(row.startDate)}
            xScale={xScale}
            yScale={yScale}
            color={colorScale}
            value={(row, key) => row[key] as number}
          >
            {(barStacks) =>
              barStacks.map((barStack) =>
                barStack.bars.map((bar) => {
                  const isHovered = tooltipData?.row === rows[bar.index];
                  const isDemoted = !isHovered && tooltipOpen;
                  return (
                    <motion.rect
                      key={`bar-${barStack.index}-${bar.index}`}
                      x={bar.x}
                      y={bar.y - margin.top}
                      height={bar.height}
                      width={bar.width}
                      fill={bar.color}
                      initial={{ opacity: 1 }}
                      animate={{ opacity: isDemoted ? 0.5 : 1 }}
                      onMouseLeave={() => {
                        tooltipTimeoutRef.current = window.setTimeout(
                          () => hideTooltip(),
                          TOOLTIP_DISMISS_TIMEOUT_MS,
                        );
                      }}
                      onMouseMove={(event) => {
                        if (tooltipTimeoutRef.current) {
                          clearTimeout(tooltipTimeoutRef.current);
                          tooltipTimeoutRef.current = undefined;
                        }
                        showTooltip({
                          ...calculateTooltipPosition({ event }),
                          tooltipData: { row: rows[bar.index] },
                        });
                      }}
                    />
                  );
                }),
              )
            }
          </BarStack>
        </Group>
        <Group transform="translate(0, 0)">
          <AxisLeft<AxisScale<number>>
            {...axisLeftProps}
            hideTicks
            scale={yScale}
            stroke={colors.border.secondary}
            tickFormat={(value, ix, values) => {
              if (ix > 0 && ix < values.length - 1) {
                return "";
              }
              return formatCurrency(value as number, {
                trailingZeroDisplay: "stripIfInteger",
                useGrouping: false,
                notation: "compact",
              });
            }}
            tickLabelProps={() => ({
              fill: colors.foreground.primary,
              fontSize: textStyles["text-small"].fontSize,
              textAnchor: "end",
            })}
          />
          <AxisBottom<AxisScale<number>>
            {...axisBottomProps}
            hideTicks
            scale={xScale}
            stroke={colors.border.secondary}
            labelOffset={8}
            tickLabelProps={() => ({
              fill: colors.foreground.primary,
              fontSize: textStyles["text-small"].fontSize,
              dy: "4px",
              textAnchor: "middle",
            })}
          />
        </Group>
      </svg>
      {tooltipOpen &&
        tooltipData &&
        tooltipTop != null &&
        tooltipLeft != null && (
          <GraphTooltip
            component={TooltipInPortal}
            top={tooltipTop}
            left={tooltipLeft}
          >
            <Box
              py="2"
              px="3"
              minWidth="180px"
              data-testid="account-spend-chart-tooltip"
            >
              <Text textStyle="text-ui-med" mb="1">
                {longDate(tooltipData.row.startDate)}
              </Text>
              {accountIds
                .filter((id) => (tooltipData.row[id] as number) > 0)
                .map((id) => (
                  <HStack key={id} justifyContent="space-between" gap="4">
                    <HStack gap="2">
                      <Box
                        width="2"
                        height="2"
                        borderRadius="sm"
                        backgroundColor={colorFor.get(id)}
                      />
                      <Text textStyle="text-small">{shortId(id)}</Text>
                    </HStack>
                    <Text textStyle="text-small">
                      {formatCurrency(tooltipData.row[id] as number)}
                    </Text>
                  </HStack>
                ))}
            </Box>
          </GraphTooltip>
        )}
    </Box>
  );
};

const AccountSpendChart = (props: {
  rows: StackedDailyRow[];
  accountIds: string[];
  colorFor: Map<string, string>;
}) => (
  <Box height={chartHeightPx} data-testid="account-spend-chart-container">
    <ParentSize className="chart-container" debounceTime={10}>
      {(parent) => (
        <AccountSpendChartGraph {...parent} height={chartHeightPx} {...props} />
      )}
    </ParentSize>
  </Box>
);

const TrendSparkline = ({
  points,
  color,
}: {
  points: number[];
  color: string;
}) => {
  const dataPoints = points.map((value, offset) => ({ offset, value }));
  const max = Math.max(0, ...points);
  return (
    <Box height="20px" width="100%">
      <ParentSize debounceTime={10}>
        {(parent) => {
          const xScale = scaleLinear({
            domain: [0, Math.max(1, dataPoints.length - 1)],
            range: [0, parent.width],
          });
          const yScale = scaleLinear({
            domain: [0, max],
            range: [
              parent.height - SPARKLINE_STROKE_WIDTH / 2,
              SPARKLINE_STROKE_WIDTH / 2,
            ],
          });
          return (
            <svg height={parent.height} width={parent.width} overflow="hidden">
              <LinePath
                data={dataPoints}
                stroke={color}
                strokeWidth={SPARKLINE_STROKE_WIDTH}
                strokeLinecap="round"
                curve={curveMonotoneX}
                x={(d) => xScale(d.offset)}
                y={(d) => yScale(d.value)}
              />
            </svg>
          );
        }}
      </ParentSize>
    </Box>
  );
};

/**
 * The all-accounts comparison table: one row per account, biggest spender
 * first, showing its share of the period total, a daily-total trend, and its
 * total cost. Clicking a row selects that account (drilldown).
 */
const ComparisonTable = ({
  days,
  accountIds,
  colorFor,
  onSelect,
}: {
  days: CostBreakdownDay[];
  accountIds: string[];
  colorFor: Map<string, string>;
  onSelect: (accountId: string) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const series = useMemo(() => accountDailyTotals(days), [days]);
  const totals = useMemo(
    () =>
      new Map(
        accountIds.map((id) => [
          id,
          (series.get(id) ?? []).reduce((sum, amount) => sum + amount, 0),
        ]),
      ),
    [accountIds, series],
  );
  const grandTotal = Array.from(totals.values()).reduce((a, b) => a + b, 0);

  const headerStyles = {
    ...baseCellStyles,
    height: 10,
    textStyle: "text-ui-med",
    color: colors.foreground.secondary,
    borderColor: colors.border.secondary,
  };
  const cellStyles = {
    ...baseCellStyles,
    borderColor: colors.border.secondary,
  };

  return (
    <Grid
      mt="6"
      gridTemplateColumns="minmax(200px, 1fr) minmax(90px, auto) minmax(120px, 1fr) minmax(90px, auto)"
      role="table"
      borderTop="1px solid"
      borderTopColor={colors.border.secondary}
    >
      <Box {...headerStyles} role="columnheader">
        Account
      </Box>
      <Box {...headerStyles} role="columnheader" justifyContent="end">
        Share
      </Box>
      <Box {...headerStyles} role="columnheader">
        Trend
      </Box>
      <Box {...headerStyles} role="columnheader" justifyContent="end">
        Cost
      </Box>
      {accountIds.map((id) => {
        const total = totals.get(id) ?? 0;
        const share = grandTotal > 0 ? (total / grandTotal) * 100 : 0;
        return (
          <Box
            key={id}
            display="contents"
            role="row"
            cursor="pointer"
            data-testid="account-comparison-row"
            onClick={() => onSelect(id)}
          >
            <Box {...cellStyles} role="cell">
              <Box
                width="2"
                height="2"
                borderRadius="sm"
                backgroundColor={colorFor.get(id)}
                marginRight="2"
                flexShrink={0}
              />
              <Tooltip label={id}>
                <Text whiteSpace="nowrap">{shortId(id)}</Text>
              </Tooltip>
            </Box>
            <Box {...cellStyles} role="cell" justifyContent="end">
              {share.toFixed(1)}%
            </Box>
            <Box {...cellStyles} role="cell">
              <TrendSparkline
                points={series.get(id) ?? []}
                color={colorFor.get(id) ?? colors.accent.purple}
              />
            </Box>
            <Box {...cellStyles} role="cell" justifyContent="end">
              {formatCurrency(total)}
            </Box>
          </Box>
        );
      })}
    </Grid>
  );
};

/**
 * Single-account drilldown: the account's clusters as a flat region-qualified
 * cost list, preserving the detail the previous "Spend by account & cluster"
 * table showed. The mock's richer resource-type drilldown (compute/storage
 * grouping, per-cluster Usage, replica size badges) is deferred: it needs
 * per-cluster usage quantities and replica sizes that `/api/costs/breakdown/
 * daily` does not carry (it returns dollar amounts per price id only). Building
 * it is blocked on that endpoint gaining those fields.
 */
const AccountClusterList = ({
  clusters,
}: {
  clusters: CostBreakdownCluster[];
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const headerStyles = {
    ...baseCellStyles,
    height: 10,
    textStyle: "text-ui-med",
    color: colors.foreground.secondary,
    borderColor: colors.border.secondary,
  };
  const cellStyles = {
    ...baseCellStyles,
    borderColor: colors.border.secondary,
  };
  return (
    <Grid
      mt="6"
      gridTemplateColumns="minmax(250px, 1fr) minmax(120px, auto)"
      role="table"
      borderTop="1px solid"
      borderTopColor={colors.border.secondary}
    >
      <Box {...headerStyles} role="columnheader">
        Cluster
      </Box>
      <Box {...headerStyles} role="columnheader" justifyContent="end">
        Total cost
      </Box>
      {clusters.map((cluster, ix) => (
        <Box key={ix} display="contents" role="row">
          <Box {...cellStyles} role="cell" whiteSpace="nowrap">
            {clusterLabel(cluster)}
          </Box>
          <Box {...cellStyles} role="cell" justifyContent="end">
            {formatCurrency(clusterTotal(cluster.amounts))}
          </Box>
        </Box>
      ))}
    </Grid>
  );
};

const AccountSwitcher = ({
  accountIds,
  selected,
  onSelect,
}: {
  accountIds: string[];
  selected: string;
  onSelect: (value: string) => void;
}) => (
  <HStack gap="2" flexWrap="wrap" data-testid="account-switcher">
    <Button
      size="sm"
      variant={selected === ALL_ACCOUNTS ? "primary" : "secondary"}
      onClick={() => onSelect(ALL_ACCOUNTS)}
    >
      All accounts
    </Button>
    {accountIds.map((id) => (
      <Tooltip key={id} label={id}>
        <Button
          size="sm"
          variant={selected === id ? "primary" : "secondary"}
          onClick={() => onSelect(id)}
        >
          {shortId(id)}
        </Button>
      </Tooltip>
    ))}
  </HStack>
);

/**
 * Per-account spend, bucketed by UTC day (Direction-B / SAS-128). An account
 * switcher toggles between the all-accounts roll-up (stacked-by-account daily
 * chart + comparison table) and a single account's drilldown. Sourced from
 * `/api/costs/breakdown/daily` via `useDailyCostsBreakdown`.
 */
const AccountSpendBreakdown = ({
  days,
  isLoading,
  isError,
  error,
}: AccountSpendBreakdownProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [selected, setSelected] = useState<string>(ALL_ACCOUNTS);

  const accountIds = useMemo(
    () => (days ? accountIdsByTotal(days) : []),
    [days],
  );
  const rows = useMemo(
    () => (days ? stackedDailyRows(days, accountIds) : []),
    [days, accountIds],
  );
  const colorFor = useAccountColors(accountIds);
  const aggregate = useMemo(() => (days ? aggregateDays(days) : null), [days]);

  // An account can vanish between renders (time-range change); fall back to the
  // roll-up rather than showing an empty drilldown.
  const activeAccount =
    selected === ALL_ACCOUNTS
      ? null
      : accountIds.includes(selected)
        ? selected
        : null;

  const legend = (
    <HStack height={legendHeightPx} spacing={4} mb="2" flexWrap="wrap">
      {accountIds.map((id) => (
        <HStack key={id} gap="2">
          <Box
            width="2"
            height="2"
            borderRadius="sm"
            backgroundColor={colorFor.get(id)}
          />
          <Text textStyle="text-small">{shortId(id)}</Text>
        </HStack>
      ))}
    </HStack>
  );

  return (
    <Box data-testid="account-spend-breakdown">
      <Text textStyle="heading-sm" mb={4}>
        Spend by account &amp; cluster
      </Text>
      {isLoading ? (
        <Spinner data-testid="account-breakdown-loading" />
      ) : isError ? (
        <ErrorBox
          message={error?.message || "There was an error fetching your usage."}
        />
      ) : !days || accountIds.length === 0 ? (
        <Text
          textStyle="text-ui-reg"
          color={colors.foreground.secondary}
          data-testid="account-breakdown-empty"
        >
          No usage to break down for the selected period.
        </Text>
      ) : (
        <>
          <AccountSwitcher
            accountIds={accountIds}
            selected={selected}
            onSelect={setSelected}
          />
          {activeAccount === null ? (
            <Box mt="4">
              {legend}
              <AccountSpendChart
                rows={rows}
                accountIds={accountIds}
                colorFor={colorFor}
              />
              <ComparisonTable
                days={days}
                accountIds={accountIds}
                colorFor={colorFor}
                onSelect={setSelected}
              />
            </Box>
          ) : (
            <Box mt="4">
              <AccountSpendChart
                rows={rows}
                accountIds={[activeAccount]}
                colorFor={colorFor}
              />
              <AccountClusterList
                clusters={
                  aggregate?.accounts.find(
                    (a) => a.external_customer_id === activeAccount,
                  )?.clusters ?? []
                }
              />
            </Box>
          )}
        </>
      )}
    </Box>
  );
};

export default AccountSpendBreakdown;
