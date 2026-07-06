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
  chakra,
  Grid,
  HStack,
  Spinner,
  Text,
  Tooltip,
  useDisclosure,
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
import React, { useMemo } from "react";

import {
  CostBreakdownAccount,
  CostBreakdownCluster,
  CostBreakdownDay,
} from "~/api/cloudGlobalApi";
import ErrorBox from "~/components/ErrorBox";
import { GraphTooltip } from "~/components/graphComponents";
import ChevronRightIcon from "~/svg/ChevronRightIcon";
import { MaterializeTheme } from "~/theme";
import { DATE_FORMAT_SHORT, formatDateInUtc } from "~/utils/dateFormat";
import { formatCurrency } from "~/utils/format";
import {
  buildXYGraphLayoutProps,
  calculateTooltipPosition,
} from "~/utils/graph";

import { baseCellStyles, resourceTypePaddingLeft } from "./constants";
import {
  accountDailyTotals,
  accountIdsByTotal,
  accountTotal,
  aggregateDays,
  clusterTotal,
  StackedDailyRow,
  stackedDailyRows,
} from "./dailyBreakdown";
import RegionSelect from "./RegionSelect";
import { SafariSafeCollapse } from "./SpendBreakdown";
import TimeRangeSelect from "./TimeRangeSelect";

const margin = { top: 10, right: 0, bottom: 36, left: 50 };
const chartHeightPx = 245;
const TOOLTIP_DISMISS_TIMEOUT_MS = 300;
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

// TODO(SAS-145): `/api/costs/breakdown/daily` returns dollar amounts only, no
// usage quantities (Orb's per-price `quantity` is discarded server-side). Until
// SAS-145 threads it through, the Usage column renders this placeholder rather
// than a number.
const USAGE_PLACEHOLDER = "—";

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
 * One account rendered as an expandable ledger group: the account is the
 * always-visible parent row (caret + color swatch + label, its share of the
 * period total, a daily-total trend, and its total cost) and its clusters are
 * indented, region-qualified child rows revealed by the disclosure. Groups
 * default open so the whole ledger is visible without interaction.
 */
const AccountLedgerGroup = ({
  account,
  total,
  share,
  trend,
  color,
}: {
  account: CostBreakdownAccount;
  total: number;
  share: number;
  trend: number[];
  color: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { isOpen, onToggle } = useDisclosure({ defaultIsOpen: true });

  const groupHeaderStyles = {
    ...baseCellStyles,
    height: 16,
    textStyle: "heading-xs",
    borderBottom: 0,
    borderTop: "1px solid",
    borderColor: colors.border.secondary,
  };

  return (
    <>
      <Box
        display="contents"
        role="row"
        onClick={onToggle}
        cursor="pointer"
        data-testid="account-row"
      >
        <Box {...groupHeaderStyles} role="cell">
          <ChevronRightIcon
            width="4"
            height="4"
            transform={`rotate(${isOpen ? 90 : 0}deg)`}
            transition="all 0.1s"
            marginRight="2"
          />
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
              {shortId(account.external_customer_id)}
            </Text>
          </Tooltip>
        </Box>
        <Box {...groupHeaderStyles} role="cell" />
        <Box {...groupHeaderStyles} role="cell" justifyContent="end">
          {share.toFixed(1)}%
        </Box>
        <Box {...groupHeaderStyles} role="cell">
          <TrendSparkline points={trend} color={color} />
        </Box>
        <Box {...groupHeaderStyles} role="cell" justifyContent="end">
          {formatCurrency(total)}
        </Box>
      </Box>
      <SafariSafeCollapse
        isCollapsed={!isOpen}
        rowCount={account.clusters.length}
      >
        {account.clusters.map((cluster, ix) => {
          const isLastElement = ix === account.clusters.length - 1;
          const cellStyles = {
            ...baseCellStyles,
            borderColor: "transparent",
            height: isLastElement ? 10 : baseCellStyles.height,
            paddingBottom: isLastElement ? "8px" : "unset",
          };
          return (
            <React.Fragment
              key={`${cluster.environment_id}/${cluster.cluster_grouping_key}/${cluster.category}/${ix}`}
            >
              <Box
                {...cellStyles}
                paddingLeft={resourceTypePaddingLeft}
                whiteSpace="nowrap"
                role="cell"
              >
                {clusterLabel(cluster)}
              </Box>
              <Box {...cellStyles} role="cell" justifyContent="end">
                {USAGE_PLACEHOLDER}
              </Box>
              <Box {...cellStyles} role="cell" />
              <Box {...cellStyles} role="cell" />
              <Box {...cellStyles} role="cell" justifyContent="end">
                {formatCurrency(clusterTotal(cluster.amounts))}
              </Box>
            </React.Fragment>
          );
        })}
      </SafariSafeCollapse>
    </>
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

  const headerStyles = {
    ...baseCellStyles,
    height: 10,
    textStyle: "text-ui-med",
    color: colors.foreground.secondary,
    borderColor: colors.border.secondary,
  };
  const totalStyles = {
    ...baseCellStyles,
    height: 12,
    textStyle: "text-ui-med",
    borderBottom: 0,
    borderTop: "1px solid",
    borderColor: colors.border.secondary,
  };

  return (
    <Grid
      mt="6"
      gridTemplateColumns="minmax(200px, 1fr) minmax(90px, auto) minmax(90px, auto) minmax(120px, 1fr) minmax(90px, auto)"
      role="table"
      borderBottom="1px solid"
      borderBottomColor={colors.border.secondary}
    >
      <Box {...headerStyles} role="columnheader">
        Account / cluster
      </Box>
      <Box {...headerStyles} role="columnheader" justifyContent="end">
        Usage
      </Box>
      <Box {...headerStyles} role="columnheader" justifyContent="end">
        Share of total
      </Box>
      <Box {...headerStyles} role="columnheader">
        Trend
      </Box>
      <Box {...headerStyles} role="columnheader" justifyContent="end">
        Cost
      </Box>
      {accounts.map((account, ix) => {
        const total = totals[ix];
        const share = grandTotal > 0 ? (total / grandTotal) * 100 : 0;
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
          />
        );
      })}
      <Box display="contents" role="row" data-testid="account-total-row">
        <Box {...totalStyles} role="cell">
          Total
        </Box>
        <Box {...totalStyles} role="cell" />
        <Box {...totalStyles} role="cell" />
        <Box {...totalStyles} role="cell" />
        <Box {...totalStyles} role="cell" justifyContent="end">
          {formatCurrency(grandTotal)}
        </Box>
      </Box>
    </Grid>
  );
};

/**
 * Per-account spend (Direction A / SAS-128). A stacked-by-account daily chart
 * over a single unified ledger: every account is an expandable row (share,
 * trend, cost) that opens to its region-qualified per-cluster costs. Sourced
 * from `/api/costs/breakdown/daily` via `useDailyCostsBreakdown`.
 *
 * The per-cluster Usage column is a placeholder (SAS-145) until the endpoint
 * carries usage quantities; the mock's Compute/Storage resource drilldown
 * (replica size) needs per-cluster replica sizes the endpoint does not yet
 * carry either, so that remains a separate follow-up.
 */
const AccountSpendBreakdown = ({
  days,
  isLoading,
  isError,
  error,
  regionFilter,
  setRegionFilter,
  timeRange,
  setTimeRange,
}: AccountSpendBreakdownProps) => {
  const { colors } = useTheme<MaterializeTheme>();

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
  const series = useMemo(
    () => (days ? accountDailyTotals(days) : new Map<string, number[]>()),
    [days],
  );

  // Order the aggregated accounts biggest-spender first, matching the chart's
  // stack order.
  const orderedAccounts = useMemo(
    () =>
      accountIds
        .map((id) =>
          aggregate?.accounts.find((a) => a.external_customer_id === id),
        )
        .filter((a): a is CostBreakdownAccount => a !== undefined),
    [accountIds, aggregate],
  );

  const totalSpend = useMemo(
    () =>
      orderedAccounts.reduce((sum, account) => sum + accountTotal(account), 0),
    [orderedAccounts],
  );

  const rangeStart = days?.length
    ? formatDateInUtc(parseISO(days[0].startDate), DATE_FORMAT_SHORT)
    : null;
  const rangeEnd = days?.length
    ? formatDateInUtc(
        parseISO(days[days.length - 1].startDate),
        DATE_FORMAT_SHORT,
      )
    : null;

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
          <Text textStyle="heading-sm" mt={6} data-testid="account-spend-range">
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
      )}
    </Box>
  );
};

export default AccountSpendBreakdown;
