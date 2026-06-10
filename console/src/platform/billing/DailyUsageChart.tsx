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
  StackProps,
  Table,
  TableContainer,
  Tbody,
  Td,
  Text,
  TextProps,
  Th,
  Thead,
  Tr,
  useTheme,
} from "@chakra-ui/react";
import { AxisBottom, AxisLeft, AxisScale } from "@visx/axis";
import { Group } from "@visx/group";
import ParentSize, {
  ParentSizeProvidedProps,
} from "@visx/responsive/lib/components/ParentSize";
import { scaleBand, scaleLinear, scaleOrdinal } from "@visx/scale";
import { BarStack } from "@visx/shape";
import { useTooltip, useTooltipInPortal } from "@visx/tooltip";
import { TooltipInPortalProps } from "@visx/tooltip/lib/hooks/useTooltipInPortal";
import { parseISO } from "date-fns/parseISO";
import { motion } from "framer-motion";
import { useAtom } from "jotai";
import React, { useMemo } from "react";

import { DailyCostKey, DailyCosts } from "~/api/cloudGlobalApi";
import { GraphTooltip } from "~/components/graphComponents";
import { cloudRegionsSelector, getRegionId } from "~/store/cloudRegions";
import { MaterializeTheme } from "~/theme";
import { formatDateInUtc } from "~/utils/dateFormat";
import { formatCurrency } from "~/utils/format";
import {
  buildXYGraphLayoutProps,
  calculateTooltipPosition,
} from "~/utils/graph";

import { aggregateByDay } from "./utils";

const margin = { top: 10, right: 0, bottom: 36, left: 50 };
export const legendHeightPx = 18;
export const chartHeightPx = 245;
const TOOLTIP_DISMISS_TIMEOUT_MS = 300;

export type DailyUsageChartProps = {
  data: DailyCosts["daily"];
  region: "all" | string;
};

function extractFormattedDateShort(dateString: string): string {
  const date = parseISO(dateString);
  return formatDateInUtc(date, "M/d");
}

function extractFormattedDateLong(dateString: string): string {
  const date = parseISO(dateString);
  return formatDateInUtc(date, "M/d/yyyy");
}

function getKeyCosts(
  day: DailyCosts["daily"][0],
  costKey: DailyCostKey,
  region: "all" | string,
): number {
  return day.costs[costKey].prices.reduce((acc, price) => {
    if (region === "all" || price.regionId === region) {
      acc += parseFloat(price.subtotal);
    }
    return acc;
  }, 0);
}

export type TooltipData = {
  bar: {
    day: DailyCosts["daily"][0];
    index: number;
    height: number;
    width: number;
    x: number;
    y: number;
    color: string;
  };
  hoveredIndex: number;
};

function calculateMaxDailySpend(
  data: DailyCosts["daily"],
  region: "all" | string,
): number {
  return data.reduce((currentMax, day) => {
    if (region === "all") {
      return Math.max(currentMax, parseFloat(day.subtotal));
    }
    let totalPrice = 0;
    for (const costKey of ["compute", "storage"] as const) {
      for (const price of day.costs[costKey].prices) {
        if (price.regionId === region) {
          totalPrice += parseFloat(price.subtotal);
        }
      }
    }
    return Math.max(currentMax, totalPrice);
  }, 0);
}

function calculateBarPadding(dayCount: number): number {
  if (dayCount > 90) {
    return 0;
  } else if (dayCount > 7) {
    return 0.25;
  }
  return 0.15;
}

export type LegendEntry = {
  key: DailyCostKey;
  label: string;
  color: string;
};

type DailyUsageChartTooltipProps = {
  top: number;
  left: number;
  legendData: LegendEntry[];
  tooltipData: TooltipData;
  availableRegions: string[];
  component: React.FC<TooltipInPortalProps>;
  allRegions: boolean;
};

const DailyUsageChartTooltip = ({
  legendData,
  tooltipData,
  availableRegions,
  allRegions,
  ...props
}: DailyUsageChartTooltipProps) => {
  const today = tooltipData.bar.day;
  return (
    <GraphTooltip {...props} data-testid="chart-tooltip">
      <Box py="2" px="3">
        <TableContainer>
          <Table variant="tooltip" data-testid="chart-tooltip-table">
            <Thead>
              <Tr>
                <Th data-testid="chart-tooltip-start-date">
                  {extractFormattedDateLong(today.startDate)}
                </Th>
                {availableRegions.map((region) => (
                  <Th key={region} isNumeric>
                    {availableRegions.length > 1 && region}
                  </Th>
                ))}
              </Tr>
            </Thead>
            <Tbody>
              {legendData.map(({ key, label, color }) => (
                <Tr key={key}>
                  <Td>
                    <LegendItem color={color}>{label}</LegendItem>
                  </Td>
                  {availableRegions.map((region) => (
                    <Td key={region} isNumeric>
                      {formatCurrency(getKeyCosts(today, key, region))}
                    </Td>
                  ))}
                </Tr>
              ))}
            </Tbody>
          </Table>
        </TableContainer>
      </Box>
    </GraphTooltip>
  );
};

const DailyUsageChartGraph = ({
  data,
  height,
  width,
  region,
}: DailyUsageChartProps & ParentSizeProvidedProps) => {
  const { svgProps, axisLeftProps, axisBottomProps, xScaleRange, yScaleRange } =
    React.useMemo(
      () =>
        buildXYGraphLayoutProps({
          width: width,
          height: height,
          margin: margin,
        }),
      [width, height],
    );
  const { colors, textStyles } = useTheme<MaterializeTheme>();
  const {
    tooltipOpen,
    tooltipLeft,
    tooltipTop,
    tooltipData,
    hideTooltip,
    showTooltip,
  } = useTooltip<TooltipData>();
  const { containerRef: tooltipContainerRef, TooltipInPortal } =
    useTooltipInPortal({
      detectBounds: true,
      scroll: true,
    });
  const aggregated = useMemo(() => aggregateByDay(data), [data]);
  const maxAmount = useMemo(
    () => calculateMaxDailySpend(aggregated, region),
    [aggregated, region],
  );
  const [cloudRegions] = useAtom(cloudRegionsSelector);
  const availableRegions =
    region === "all"
      ? Array.from(cloudRegions.values()).map((details) => getRegionId(details))
      : [region];
  const barPadding = calculateBarPadding(aggregated.length);
  const xScale = useMemo(
    () =>
      scaleBand<string>({
        domain: aggregated.map((day) =>
          extractFormattedDateShort(day.startDate),
        ),
        range: xScaleRange as [number, number],
        padding: barPadding,
      }),
    [aggregated, xScaleRange, barPadding],
  );
  const yScale = useMemo(
    () =>
      scaleLinear({
        // We apply a ceiling to the number so we can drop the fractional amounts
        // from the legend
        domain: [0, Math.ceil(maxAmount)],
        range: yScaleRange,
        // We want a "nice" looking tick scale that ends on a good number.
        nice: true,
      }),
    [maxAmount, yScaleRange],
  );
  const legendData: LegendEntry[] = [
    { key: "compute", label: "Compute", color: colors.accent.purple },
    { key: "storage", label: "Storage", color: colors.border.secondary },
  ];
  const colorScale = scaleOrdinal<string, string>({
    domain: legendData.map((cat) => cat.label),
    range: legendData.map((cat) => cat.color),
  });
  const tooltipTimeoutRef = React.useRef<number>();
  return (
    <Box position="relative">
      <HStack data-testid="legend" spacing={4} as="ul" height={legendHeightPx}>
        {legendData.map(({ key, label, color }) => (
          <LegendItem textProps={{ fontSize: "xs" }} key={key} color={color}>
            {label}
          </LegendItem>
        ))}
      </HStack>
      <svg data-testid="chart" ref={tooltipContainerRef} {...svgProps}>
        <Group top={margin.top}>
          <BarStack<DailyCosts["daily"][0], DailyCostKey>
            data={aggregated}
            keys={["compute", "storage"]}
            x={(day) => extractFormattedDateShort(day.startDate)}
            xScale={xScale}
            yScale={yScale}
            color={colorScale}
            value={(datum, key) => getKeyCosts(datum, key, region)}
          >
            {(barStacks) =>
              barStacks.map((barStack) =>
                barStack.bars.map((bar, stackIx) => {
                  const isHighlighted = tooltipData?.hoveredIndex === stackIx;
                  const isDemoted = !isHighlighted && tooltipOpen;
                  return (
                    <motion.rect
                      key={`bar-stack-${barStack.index}-${bar.index}`}
                      x={bar.x}
                      y={bar.y - margin.top}
                      height={bar.height}
                      width={bar.width}
                      fill={bar.color}
                      initial={{ opacity: isDemoted ? 1 : 0 }}
                      animate={{ opacity: isDemoted ? 0.5 : 1 }}
                      onMouseLeave={() => {
                        tooltipTimeoutRef.current = window.setTimeout(
                          () => {
                            hideTooltip();
                          },
                          // Hold off on dismissing the tooltip to give a little
                          // accuracy leeway to users when they're mousing over
                          // the bar.
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
                          tooltipData: {
                            bar: {
                              day: aggregated[stackIx],
                              index: barStack.index,
                              x: bar.x,
                              y: bar.y,
                              height: bar.height,
                              width: bar.width,
                              color: bar.color,
                            },
                            hoveredIndex: stackIx,
                          },
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
              // Only label the first and last ticks.
              if (ix > 0 && ix < values.length - 1) {
                return "";
              }
              return formatCurrency(value, {
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
      {tooltipTop && tooltipLeft && tooltipOpen && tooltipData && (
        <DailyUsageChartTooltip
          component={TooltipInPortal}
          top={tooltipTop}
          left={tooltipLeft}
          legendData={legendData}
          tooltipData={tooltipData}
          availableRegions={availableRegions}
          allRegions={region === "all"}
        />
      )}
    </Box>
  );
};

const LegendItem = ({
  children,
  color,
  textProps,
  ...props
}: React.PropsWithChildren<
  {
    color: string;
    textProps?: TextProps;
  } & StackProps
>) => {
  return (
    <HStack {...props}>
      <Box width={2} height={2} borderRadius="sm" backgroundColor={color}></Box>
      <Text {...textProps}>{children}</Text>
    </HStack>
  );
};

const DailyUsageChart = (props: DailyUsageChartProps) => {
  return (
    <ParentSize className="chart-container" debounceTime={10}>
      {(parentSizeState) => {
        return (
          <DailyUsageChartGraph
            {...parentSizeState}
            height={chartHeightPx}
            {...props}
          />
        );
      }}
    </ParentSize>
  );
};

export { DailyUsageChartTooltip };
export default DailyUsageChart;
