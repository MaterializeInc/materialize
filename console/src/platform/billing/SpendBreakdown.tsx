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
  BoxProps,
  chakra,
  Collapse,
  Fade,
  Grid,
  Text,
  Tooltip,
  useDisclosure,
  useTheme,
} from "@chakra-ui/react";
import { curveMonotoneX } from "@visx/curve";
import ParentSize from "@visx/responsive/lib/components/ParentSize";
import { scaleLinear } from "@visx/scale";
import { LinePath } from "@visx/shape";
import { parseISO } from "date-fns";
import React, { PropsWithChildren, useMemo } from "react";

import { DailyCostKey, DailyCosts } from "~/api/cloudGlobalApi";
import ChevronRightIcon from "~/svg/ChevronRightIcon";
import WarningIcon from "~/svg/WarningIcon";
import { MaterializeTheme } from "~/theme";
import { formatBytes, isSafari } from "~/util";
import { DATE_FORMAT_SHORT, formatDateInUtc } from "~/utils/dateFormat";
import { formatCurrency } from "~/utils/format";

import { costUnits } from "./constants";
import { ResourceBreakdown, ResourceMeasurement } from "./types";
import { summarizeResourceCosts } from "./utils";

// The first day of plans that introduced storage usage bucketed by region.
const BUCKETED_STORAGE_START_DATE = new Date("2024-02-01T00:00:00Z");

type SpendBreakdownProps = {
  region: "all" | string;
  dailyCosts: DailyCosts["daily"] | null;
  totalDays: number;
};

type ResourceGroupProps = {
  costCaption?: boolean;
  costKey: DailyCostKey;
  headerProps?: BoxProps;
  isLoading: boolean;
  isRegionFiltering: boolean;
  label: string;
  selectedRegions: Array<[string, ResourceBreakdown]>;
  warningMessage?: string;
};

function safeRound(value: number, digits: number): number {
  const rounded = Math.round(Number(`${value}e+${digits}`));
  return Number(`${rounded}e-${digits}`);
}

const STROKE_WIDTH = 2;

const SpendSparkline = ({
  color,
  points,
  min,
  max,
  height,
  width,
}: {
  points: number[];
  min: number;
  max: number;
  color: string;
  height: number;
  width: number;
}) => {
  const dataPoints = points.map((value, ix) => ({ offset: ix, value }));
  const xScale = useMemo(
    () =>
      scaleLinear({
        // Cartesian system values (ascending left to right)
        domain: [0, dataPoints.length],
        // SVG coordinate values (ascending left to right)
        range: [0, width],
      }),
    [width, dataPoints],
  );
  const yScale = useMemo(
    () =>
      scaleLinear({
        // Cartesian system values (ascending left to right)
        domain: [min, max],
        // SVG coordinate values (ascending top to bottom)
        // Add extra padding to avoid clipping broad strokes.
        range: [height - STROKE_WIDTH / 2, STROKE_WIDTH / 2],
      }),
    [height, min, max],
  );
  return (
    <svg height={height} width={width} overflow="hidden">
      <LinePath
        data={dataPoints}
        stroke={color}
        strokeWidth={STROKE_WIDTH}
        strokeLinecap="round"
        curve={curveMonotoneX}
        x={(d) => xScale(d.offset)}
        y={(d) => yScale(d.value)}
      />
    </svg>
  );
};

const StorageMetric = ({ usageGb }: { usageGb: number }) => {
  const usageBytes = usageGb * Math.pow(1024, 3);
  const [amount, unit] = formatBytes(usageBytes);
  const rounded = safeRound(amount, 2);
  return (
    <Tooltip label={`${amount} ${unit}`} aria-label="An amount of storage">
      <span>
        {rounded} {unit}
      </span>
    </Tooltip>
  );
};

const CreditsMetric = ({ credits }: { credits: number }) => {
  const rounded = safeRound(credits, 2);
  return (
    <Tooltip label={`${credits} credits`} aria-label="An amount of credits">
      <span>{rounded} credits</span>
    </Tooltip>
  );
};

const resourceTypePaddingLeft = 4 + 4 + 2; // table cell + width of caret + caret/label gap

const baseCellStyles = {
  px: 4,
  my: "auto",
  display: "flex",
  alignItems: "center",
  height: 8,
  borderBottom: "1px solid",
};

function getMinWithDefault(left: number, right: number): number {
  /// We initialize min values with -1, so it always loses.
  if (left === -1) {
    return right;
  }
  if (right === -1) {
    return left;
  }
  return Math.min(left, right);
}

function findMinMax(points: number[]) {
  let min = 1;
  let max = 0;
  for (const point of points) {
    min = getMinWithDefault(min, point);
    max = Math.max(max, point);
  }
  return { min, max };
}

type ResourceRowProps = {
  costCaption?: boolean;
  regionId: string;
  resourceType: string;
  resourceSummary: ResourceMeasurement;
  isLastElement: boolean;
  isRegionFiltering: boolean;
};

const ResourceRow = ({
  regionId,
  resourceType,
  resourceSummary,
  isLastElement,
  isRegionFiltering,
  costCaption,
}: ResourceRowProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const cellStyles = {
    ...baseCellStyles,
    borderColor: "transparent",
    height: isLastElement ? 10 : baseCellStyles.height,
    paddingBottom: isLastElement ? "8px" : "unset",
  };
  const { min: usageMin, max: usageMax } = useMemo(
    () => findMinMax(resourceSummary.usagePoints),
    [resourceSummary],
  );
  return (
    <>
      <Box
        {...cellStyles}
        paddingLeft={resourceTypePaddingLeft}
        whiteSpace="nowrap"
        role="cell"
      >
        {!isRegionFiltering && `${regionId} / `}
        {resourceType}
      </Box>
      <Box {...cellStyles} role="cell" whiteSpace="nowrap">
        {resourceSummary.usageUnits === costUnits.storage ? (
          <StorageMetric usageGb={resourceSummary.usageValue} />
        ) : (
          <CreditsMetric credits={resourceSummary.usageValue} />
        )}
      </Box>
      <Box {...cellStyles} role="cell">
        <Box height="20px" width="100%">
          <ParentSize debounceTime={10}>
            {(parent) => (
              <SpendSparkline
                points={resourceSummary.usagePoints}
                min={usageMin}
                max={usageMax}
                color={colors.border.secondary}
                height={parent.height}
                width={parent.width}
              />
            )}
          </ParentSize>
        </Box>
      </Box>
      <Box
        {...cellStyles}
        flexDirection="column"
        alignItems="end"
        justifyContent="center"
        role="cell"
      >
        {formatCurrency(resourceSummary.totalCost)}
        {costCaption && (
          <Text
            textStyle="text-small"
            fontSize="10px"
            color={colors.foreground.tertiary}
            whiteSpace="nowrap"
          >
            (${resourceSummary.rate} {resourceSummary.usageUnits}
            /hr)
          </Text>
        )}
      </Box>
    </>
  );
};

function summarizeRegions(regions: ResourceBreakdown[], costKey: DailyCostKey) {
  let totalPoints: number[] = [];
  for (const region of regions) {
    const { usagePoints: regionPoints } = region[costKey].total;
    if (totalPoints.length === 0) {
      totalPoints = Array(regionPoints.length).fill(0);
    }
    totalPoints = regionPoints.map((val, ix) => val + totalPoints[ix]);
  }
  let min = -1;
  let max = 0;
  for (const point of totalPoints) {
    min = getMinWithDefault(min, point);
    max = Math.max(max, point);
  }
  return { min, max, totalPoints };
}

const ResourceGroup = ({
  costCaption,
  costKey,
  headerProps,
  isLoading,
  isRegionFiltering,
  label,
  selectedRegions,
  warningMessage,
}: ResourceGroupProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { isOpen, onToggle } = useDisclosure();
  const totalUsageMetric = selectedRegions.reduce(
    (total, [_regionId, regionSummary]) =>
      total + regionSummary[costKey].total.usageValue,
    0,
  );
  const selectedSummary = summarizeRegions(
    selectedRegions.map(([_regionId, breakdown]) => breakdown),
    costKey,
  );
  const resourceMeasurements = selectedRegions.flatMap(
    ([regionId, regionSummary]) => {
      return Array.from(regionSummary[costKey].resources.entries())
        .sort(([_l, left], [_r, right]) => left.sort - right.sort)
        .map(
          ([resourceType, resourceSummary]) =>
            [regionId, resourceType, resourceSummary] as const,
        );
    },
  );
  const groupHeaderStyles = {
    ...baseCellStyles,
    height: 16,
    textStyle: "heading-xs",
    borderColor: !isOpen ? colors.border.secondary : "transparent",
    borderBottom: 0,
  };
  return (
    <>
      <Box
        data-testid={`spend-breakdown-${costKey}-group-header`}
        display="contents"
        role="row"
        onClick={onToggle}
        cursor="pointer"
      >
        <Box {...groupHeaderStyles} role="cell" {...headerProps}>
          <ChevronRightIcon
            width="4"
            height="4"
            transform={`rotate(${!isOpen || isLoading ? 0 : 90}deg)`}
            transition="all 0.1s"
            marginRight="2"
          />
          {label}
          <Fade in={!!warningMessage}>
            <Tooltip label={warningMessage}>
              <WarningIcon marginLeft={2} stroke={colors.accent.darkYellow} />
            </Tooltip>
          </Fade>
        </Box>
        <Box {...groupHeaderStyles} role="cell" {...headerProps}>
          {selectedRegions.length > 0 && costKey === "storage" ? (
            <StorageMetric usageGb={totalUsageMetric} />
          ) : (
            <CreditsMetric credits={totalUsageMetric} />
          )}
        </Box>
        <Box {...groupHeaderStyles} role="cell" {...headerProps}>
          <Box height="20px" width="100%">
            <ParentSize debounceTime={10}>
              {(parent) => (
                <SpendSparkline
                  points={selectedSummary.totalPoints}
                  min={selectedSummary.min}
                  max={selectedSummary.max}
                  color={colors.accent.purple}
                  height={parent.height}
                  width={parent.width}
                />
              )}
            </ParentSize>
          </Box>
        </Box>
        <Box {...groupHeaderStyles} {...headerProps} justifyContent="end">
          {formatCurrency(
            selectedRegions.reduce(
              (total, [_regionId, regionSummary]) =>
                total + regionSummary[costKey].total.totalCost,
              0,
            ),
          )}
        </Box>
      </Box>
      <SafariSafeCollapse
        isCollapsed={!isOpen || isLoading}
        rowCount={resourceMeasurements.length}
        data-testid={`spend-breakdown-${costKey}-group`}
      >
        {resourceMeasurements.map(
          ([regionId, resourceType, resourceSummary], ix) => (
            <ResourceRow
              key={ix}
              regionId={regionId}
              resourceType={resourceType}
              resourceSummary={resourceSummary}
              costCaption={costCaption}
              isLastElement={ix === resourceMeasurements.length - 1}
              isRegionFiltering={isRegionFiltering}
            />
          ),
        )}
      </SafariSafeCollapse>
    </>
  );
};

/**
 * Safari fails to layout the subgrid elements correctly if they're in a
 * Collapse component. For Safari, fall back to showing and hiding a simple
 * div. Other browsers get the nice animation.
 *
 * StackOverflow post where someone encounters a similar issue:
 *  https://stackoverflow.com/q/77927259/214197
 */
const SafariSafeCollapse = ({
  children,
  isCollapsed,
  rowCount,
  ...otherProps
}: PropsWithChildren<{
  isCollapsed: boolean;
  rowCount: number;
}>) => {
  const collapseProps = {
    display: "grid",
    gridTemplateColumns: "subgrid",
    gridColumn: "1 / -1",
    maxHeight: rowCount * 32 + 8,
  };
  if (isSafari()) {
    return (
      <Box
        {...collapseProps}
        display={isCollapsed ? "none" : "grid"}
        {...otherProps}
      >
        {children}
      </Box>
    );
  } else {
    return (
      <Collapse in={!isCollapsed} style={collapseProps} {...otherProps}>
        {children}
      </Collapse>
    );
  }
};

const SpendBreakdown = ({
  region,
  dailyCosts,
  totalDays,
}: SpendBreakdownProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const isLoading = dailyCosts === null;
  const startDate = !isLoading
    ? formatDateInUtc(parseISO(dailyCosts[0].startDate), DATE_FORMAT_SHORT)
    : "-";
  const endDate = !isLoading
    ? formatDateInUtc(parseISO(dailyCosts.at(-1)!.startDate), DATE_FORMAT_SHORT)
    : "-";
  const hasUnbucketedStorage =
    !isLoading &&
    parseISO(dailyCosts[0].startDate) < BUCKETED_STORAGE_START_DATE;
  const resourceCostsByRegion = useMemo(
    () => summarizeResourceCosts(dailyCosts ?? [], totalDays),
    [dailyCosts, totalDays],
  );
  const isRegionFiltering = region !== "all";
  const regions = !isRegionFiltering
    ? Array.from(resourceCostsByRegion.keys())
    : [region];
  const selectedRegions = Array.from(resourceCostsByRegion.entries()).filter(
    ([regionId, _regionSummary]) => regions.includes(regionId),
  );

  const tableHeaderStyles = {
    ...baseCellStyles,
    py: 3,
    textStyle: "text-ui-med",
    color: colors.foreground.secondary,
    borderColor: colors.border.secondary,
    height: 10,
  };

  return (
    <Box>
      <Text textStyle="heading-sm" data-testid="spend-breakdown-range">
        Spend between{" "}
        {!isLoading && (
          <>
            <chakra.time dateTime={startDate} color={colors.accent.green}>
              {startDate}
            </chakra.time>{" "}
            and{" "}
            <chakra.time dateTime={endDate} color={colors.accent.green}>
              {endDate}
            </chakra.time>
          </>
        )}
      </Text>
      <Grid
        mt="6"
        gridTemplateColumns="minmax(250px, 25%) repeat(3, minmax(20%, 25%))"
        gridTemplateRows="40px repeat(auto-fill, minmax(0, 64px))"
        role="table"
        borderBottom="1px solid"
        borderBottomColor={colors.border.secondary}
      >
        <Box
          {...tableHeaderStyles}
          paddingLeft={resourceTypePaddingLeft}
          role="columnheader"
        >
          Resource type
        </Box>
        <Box {...tableHeaderStyles} role="columnheader">
          Usage
        </Box>
        <Box {...tableHeaderStyles} role="columnheader">
          Usage trend
        </Box>
        <Box {...tableHeaderStyles} role="columnheader" justifyContent="end">
          Total cost
        </Box>
        <ResourceGroup
          label="Compute"
          costKey="compute"
          isRegionFiltering={isRegionFiltering}
          selectedRegions={selectedRegions}
          isLoading={isLoading}
        />
        <ResourceGroup
          label="Storage"
          costKey="storage"
          isRegionFiltering={isRegionFiltering}
          selectedRegions={selectedRegions}
          costCaption={true}
          headerProps={{
            borderTop: "1px solid",
            borderTopColor: colors.border.secondary,
          }}
          isLoading={isLoading}
          warningMessage={
            hasUnbucketedStorage
              ? `Storage usage prior to ${formatDateInUtc(
                  BUCKETED_STORAGE_START_DATE,
                  DATE_FORMAT_SHORT,
                )} is not captured here. Consult your invoices for storage usage and billing prior to this date.`
              : undefined
          }
        />
      </Grid>
    </Box>
  );
};

export default SpendBreakdown;
