// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, chakra, HStack, Text, Tooltip, useTheme } from "@chakra-ui/react";
import { AxisBottom, AxisLeft, AxisScale } from "@visx/axis";
import { curveMonotoneX } from "@visx/curve";
import { localPoint } from "@visx/event";
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

import {
  CostBreakdownAccount,
  CostBreakdownCluster,
  CostBreakdownDay,
} from "~/api/cloudGlobalApi";
import ErrorBox from "~/components/ErrorBox";
import { GraphEventOverlay, GraphTooltip } from "~/components/graphComponents";
import { LoadingContainer } from "~/components/LoadingContainer";
import {
  EmptyListHeader,
  EmptyListHeaderContents,
  EmptyListWrapper,
} from "~/layouts/listPageComponents";
import { MaterializeTheme } from "~/theme";
import { DATE_FORMAT_SHORT, formatDateInUtc } from "~/utils/dateFormat";
import { formatCurrency, formatPercentage } from "~/utils/format";
import {
  buildXYGraphLayoutProps,
  calculateTooltipPosition,
} from "~/utils/graph";

import { ACCOUNT_SPEND_FETCH_ERROR_MESSAGE } from "./constants";
import {
  accountTotal,
  clusterTotal,
  pivotBreakdown,
  shortAccountId,
  StackedDailyRow,
} from "./dailyBreakdown";
import {
  LedgerCaret,
  LedgerCell,
  LedgerGroup,
  LedgerTable,
} from "./LedgerTable";
import RegionSelect from "./RegionSelect";
import TimeRangeSelect from "./TimeRangeSelect";

const margin = { top: 10, right: 0, bottom: 36, left: 50 };
const chartHeightPx = 245;
const SPARKLINE_STROKE_WIDTH = 2;

type AccountSpendBreakdownProps = {
  days: CostBreakdownDay[] | null;
  isLoading: boolean;
  isError: boolean;
  error: Error | null;
  regionFilter: "all" | string;
  setRegionFilter: (val: "all" | string) => void;
  timeRange: number;
  setTimeRange: (val: number) => void;
};

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

const usageNumberFormatter = Intl.NumberFormat("default", {
  maximumFractionDigits: 2,
});

/**
 * A cluster's usage with its unit: credits for a compute row (empty
 * `category`), GB for a storage/egress row (see `clusterLabel`).
 */
function formatUsage(cluster: CostBreakdownCluster): string {
  const unit = cluster.category ? "GB" : "credits";
  return `${usageNumberFormatter.format(cluster.usage)} ${unit}`;
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
  const {
    svgProps,
    axisLeftProps,
    axisBottomProps,
    graphEventOverlayProps,
    xScaleRange,
    yScaleRange,
  } = useMemo(
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

  // scaleBand has no invert() (which findNearestDatum requires), so map the
  // pointer's x to a column by stepping from the band range's start.
  const handlePointerMove = (event: React.PointerEvent<SVGRectElement>) => {
    const svgPoint = localPoint(event);
    if (!svgPoint || rows.length === 0) return;
    const [rangeStart] = xScale.range();
    const index = Math.min(
      rows.length - 1,
      Math.max(0, Math.floor((svgPoint.x - rangeStart) / xScale.step())),
    );
    showTooltip({
      ...calculateTooltipPosition({ event }),
      tooltipData: { row: rows[index] },
    });
  };

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
        <GraphEventOverlay
          {...graphEventOverlayProps}
          onPointerMove={handlePointerMove}
          onPointerLeave={() => hideTooltip()}
        />
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
                      <Text textStyle="text-small">{shortAccountId(id)}</Text>
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
 * One account rendered as an expandable ledger group: the account is the
 * always-visible parent row (caret + color swatch + label, its share of the
 * period total, a daily-total trend, and its total cost) and its clusters are
 * indented, region-qualified child rows revealed by the disclosure. Groups
 * default closed, so the ledger opens to a compact summary and clusters are
 * revealed on demand.
 */
const AccountLedgerGroup = ({
  account,
  total,
  share,
  trend,
  color,
  onOpenChange,
}: {
  account: CostBreakdownAccount;
  total: number;
  share: number;
  trend: number[];
  color: string;
  onOpenChange: (isOpen: boolean) => void;
}) => {
  return (
    <LedgerGroup
      rowCount={account.clusters.length}
      defaultIsOpen={false}
      onOpenChange={onOpenChange}
      data-testid="account-row"
      renderHeader={(isOpen) => (
        <>
          <LedgerCell variant="groupHeader">
            <LedgerCaret isOpen={isOpen} />
            <Box
              width="2"
              height="2"
              borderRadius="sm"
              backgroundColor={color}
              marginRight="2"
              flexShrink={0}
            />
            <Tooltip label={account.external_customer_id}>
              <Text whiteSpace="nowrap">
                {account.name || shortAccountId(account.external_customer_id)}
              </Text>
            </Tooltip>
          </LedgerCell>
          <LedgerCell variant="groupHeader" />
          <LedgerCell variant="groupHeader" numeric>
            {formatPercentage(share, 1)}
          </LedgerCell>
          <LedgerCell variant="groupHeader">
            <TrendSparkline points={trend} color={color} />
          </LedgerCell>
          <LedgerCell variant="groupHeader" numeric>
            {formatCurrency(total)}
          </LedgerCell>
        </>
      )}
    >
      {account.clusters.map((cluster, ix) => {
        const isLastElement = ix === account.clusters.length - 1;
        return (
          <React.Fragment
            key={`${cluster.environment_id}/${cluster.cluster_grouping_key}/${cluster.category}/${ix}`}
          >
            <LedgerCell indented isLastRow={isLastElement}>
              {clusterLabel(cluster)}
            </LedgerCell>
            <LedgerCell isLastRow={isLastElement} numeric>
              {formatUsage(cluster)}
            </LedgerCell>
            <LedgerCell isLastRow={isLastElement} />
            <LedgerCell isLastRow={isLastElement} />
            <LedgerCell isLastRow={isLastElement} numeric>
              {formatCurrency(clusterTotal(cluster.amounts))}
            </LedgerCell>
          </React.Fragment>
        );
      })}
    </LedgerGroup>
  );
};

/**
 * Direction-A unified ledger (SAS-128): one table listing every account
 * (biggest spender first) as an expandable row — its share of the period total,
 * a daily-total trend, and its cost — expanding to its region-qualified
 * per-cluster costs. A final Total row sums every account.
 */
const UnifiedLedger = ({
  accounts,
  series,
  colorFor,
}: {
  accounts: CostBreakdownAccount[];
  series: Map<string, number[]>;
  colorFor: Map<string, string>;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const totals = accounts.map((account) => accountTotal(account));
  const grandTotal = totals.reduce((sum, total) => sum + total, 0);
  // Tracks which accounts are currently expanded, so the shared Usage header
  // (SAS-169) can hide itself while nothing underneath it has a value to show.
  const [openAccountIds, setOpenAccountIds] = useState<Set<string>>(
    () => new Set(),
  );
  const anyOpen = openAccountIds.size > 0;

  return (
    <LedgerTable
      mt="6"
      templateColumns="minmax(200px, 1fr) minmax(90px, auto) minmax(90px, auto) minmax(120px, 1fr) minmax(90px, auto)"
      columns={[
        { label: "Account / cluster" },
        { label: anyOpen ? "Usage" : null, numeric: true },
        { label: "Share of total", numeric: true },
        { label: "Trend" },
        { label: "Cost", numeric: true },
      ]}
    >
      {accounts.map((account, ix) => {
        const total = totals[ix];
        const share = grandTotal > 0 ? total / grandTotal : 0;
        return (
          <AccountLedgerGroup
            key={account.external_customer_id}
            account={account}
            total={total}
            share={share}
            trend={series.get(account.external_customer_id) ?? []}
            color={
              colorFor.get(account.external_customer_id) ?? colors.accent.purple
            }
            onOpenChange={(isOpen) =>
              setOpenAccountIds((prev) => {
                const next = new Set(prev);
                if (isOpen) {
                  next.add(account.external_customer_id);
                } else {
                  next.delete(account.external_customer_id);
                }
                return next;
              })
            }
          />
        );
      })}
      <Box display="contents" role="row" data-testid="account-total-row">
        <LedgerCell variant="total">Total</LedgerCell>
        <LedgerCell variant="total" />
        <LedgerCell variant="total" />
        <LedgerCell variant="total" />
        <LedgerCell variant="total" numeric>
          {formatCurrency(grandTotal)}
        </LedgerCell>
      </Box>
    </LedgerTable>
  );
};

/**
 * Per-account spend (Direction A / SAS-128). A stacked-by-account daily chart
 * over a single unified ledger: every account is an expandable row (share,
 * trend, cost) that opens to its region-qualified per-cluster costs. Sourced
 * from `/api/costs/breakdown/daily` via `useDailyCostsBreakdown`.
 *
 * The mock's Compute/Storage resource drilldown (replica size) needs
 * per-cluster replica sizes the endpoint does not yet carry, so that remains
 * a separate follow-up.
 */
// Shared pivot of the dense day series into everything the chart and the
// ledger render: account order (biggest spender first), colors, stacked chart
// rows, per-account sparkline series, aggregated accounts, period total, and
// the date range. The chart and the ledger live in separate grid cells
// (SAS-154), so both derive from this one hook and can never disagree on
// ordering or colors.
function useAccountSpendPivot(
  days: CostBreakdownDay[] | null,
  regionFilter: "all" | string,
) {
  const { accountIds, rows, orderedAccounts, series, totalSpend } = useMemo(
    () => pivotBreakdown(days, regionFilter),
    [days, regionFilter],
  );
  const colorFor = useAccountColors(accountIds);

  const rangeStart = days?.length
    ? formatDateInUtc(parseISO(days[0].startDate), DATE_FORMAT_SHORT)
    : null;
  const rangeEnd = days?.length
    ? formatDateInUtc(
        parseISO(days[days.length - 1].startDate),
        DATE_FORMAT_SHORT,
      )
    : null;

  return {
    accountIds,
    rows,
    colorFor,
    series,
    orderedAccounts,
    totalSpend,
    rangeStart,
    rangeEnd,
  };
}

const EmptyState = () => (
  <EmptyListWrapper padding="4" data-testid="account-breakdown-empty">
    <EmptyListHeader>
      <EmptyListHeaderContents
        title="No usage to break down"
        helpText="There was no spend in the selected period and region."
      />
    </EmptyListHeader>
  </EmptyListWrapper>
);

const AccountSpendBreakdown = ({
  days,
  isLoading,
  isError,
  regionFilter,
  setRegionFilter,
  timeRange,
  setTimeRange,
}: AccountSpendBreakdownProps) => {
  const { accountIds, rows, colorFor, totalSpend } = useAccountSpendPivot(
    days,
    regionFilter,
  );

  return (
    <Box data-testid="account-spend-breakdown">
      <HStack gap={4} mb={4}>
        <div data-testid="account-region-select">
          <RegionSelect region={regionFilter} setRegion={setRegionFilter} />
        </div>
        <div data-testid="account-time-range-select">
          <TimeRangeSelect timeRange={timeRange} setTimeRange={setTimeRange} />
        </div>
      </HStack>
      {isLoading ? (
        // Reserve the chart's height so the section doesn't jump when the
        // data lands.
        <LoadingContainer minHeight={`${chartHeightPx}px`} />
      ) : isError ? (
        <ErrorBox message={ACCOUNT_SPEND_FETCH_ERROR_MESSAGE} />
      ) : !days || accountIds.length === 0 ? (
        <EmptyState />
      ) : (
        <Box mt="4">
          <Text
            as="h4"
            textStyle="heading-md"
            mb={3}
            data-testid="account-spend-total"
          >
            {formatCurrency(totalSpend)}
          </Text>
          <AccountSpendChart
            rows={rows}
            accountIds={accountIds}
            colorFor={colorFor}
          />
        </Box>
      )}
    </Box>
  );
};

export type AccountSpendLedgerProps = {
  days: CostBreakdownDay[] | null;
  isLoading: boolean;
  isError: boolean;
  regionFilter: "all" | string;
};

/**
 * The "Spend between …" heading and the unified per-account ledger, split from
 * AccountSpendBreakdown so the page can give the table its own full-width grid
 * row beneath the chart and Plan details columns (SAS-154). Loading, error,
 * and empty states are surfaced by AccountSpendBreakdown, so this renders
 * nothing in those states.
 */
export const AccountSpendLedger = ({
  days,
  isLoading,
  isError,
  regionFilter,
}: AccountSpendLedgerProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const {
    accountIds,
    colorFor,
    series,
    orderedAccounts,
    rangeStart,
    rangeEnd,
  } = useAccountSpendPivot(days, regionFilter);

  if (isLoading || isError || !days || accountIds.length === 0) {
    return null;
  }

  return (
    <Box data-testid="account-spend-ledger">
      <Text textStyle="heading-sm" data-testid="account-spend-range">
        Spend between{" "}
        {rangeStart && rangeEnd && (
          <>
            <chakra.time dateTime={rangeStart} color={colors.accent.green}>
              {rangeStart}
            </chakra.time>{" "}
            and{" "}
            <chakra.time dateTime={rangeEnd} color={colors.accent.green}>
              {rangeEnd}
            </chakra.time>
          </>
        )}
      </Text>
      <UnifiedLedger
        accounts={orderedAccounts}
        series={series}
        colorFor={colorFor}
      />
    </Box>
  );
};

export default AccountSpendBreakdown;
