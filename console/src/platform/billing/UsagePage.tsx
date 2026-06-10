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
  Divider,
  Flex,
  Grid,
  GridItem,
  HStack,
  Spinner,
  Text,
  useTheme,
} from "@chakra-ui/react";
import React, { useMemo, useRef, useState } from "react";

import { useCurrentOrganization } from "~/api/auth";
import { DailyCosts, PlanType } from "~/api/cloudGlobalApi";
import ErrorBox from "~/components/ErrorBox";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { MaterializeTheme } from "~/theme";
import { nowUTC } from "~/util";
import { formatCurrency } from "~/utils/format";

import DailyUsageChart, {
  chartHeightPx,
  legendHeightPx,
} from "./DailyUsageChart";
import InvoiceTable from "./InvoiceTable";
import { UpgradedPlanDetails } from "./PlanDetails";
import { useDailyCosts, useRecentInvoices } from "./queries";
import RegionSelect from "./RegionSelect";
import SpendBreakdown from "./SpendBreakdown";
import TimeRangeSelect from "./TimeRangeSelect";
import { getTimeRangeSlice } from "./utils";

const DEFAULT_TIME_RANGE_LOOKBACK_DAYS = 7;

const InvoiceTableWrapper = ({ planType }: { planType: PlanType }) => {
  const { data: invoices, isLoading, isError, error } = useRecentInvoices();
  return (
    <>
      {isLoading ? (
        <Spinner />
      ) : isError ? (
        <ErrorBox message={error.message} />
      ) : (
        <InvoiceTable invoices={invoices ?? []} planType={planType} />
      )}
    </>
  );
};

const AwsMarketplaceInvoiceBanner = () => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Flex
      data-testid="aws-marketplace-banner"
      backgroundColor={colors.background.secondary}
      border="1px solid"
      borderColor={colors.border.secondary}
      gap={4}
      padding={4}
      justifyContent="space-between"
      borderRadius="lg"
      marginBottom={4}
    >
      <Box>
        <Text textStyle="text-ui-med">Procured through AWS</Text>
        <Text textStyle="text-ui-reg">
          Invoices are tracked in your company&apos;s AWS Billing Console.
        </Text>
      </Box>
      <HStack gap={4}>
        <Divider orientation="vertical" borderColor={colors.border.info} />
        <Button
          variant="secondary"
          as="a"
          target="_blank"
          rel="noopener"
          href="https://console.aws.amazon.com/billing/home"
        >
          View invoices
        </Button>
      </HStack>
    </Flex>
  );
};

type ChartPanelProps = {
  timeRange: number;
  setTimeRange: (val: number) => void;
  dailyCosts: DailyCosts["daily"] | null;
  isLoading: boolean;
  isError: boolean;
  regionFilter: "all" | string;
  setRegionFilter: (val: ChartPanelProps["regionFilter"]) => void;
  error: Error | null;
};

function getTotalSpend(
  dailyCosts: DailyCosts["daily"] | null,
  region: "all" | string,
): number {
  if (dailyCosts === null) {
    return 0;
  }
  let totalPrice = 0;
  for (const day of dailyCosts) {
    for (const costKey of ["compute", "storage"] as const) {
      for (const price of day.costs[costKey].prices) {
        if (region === "all" || price.regionId === region) {
          totalPrice += parseFloat(price.subtotal);
        }
      }
    }
  }
  return totalPrice;
}

const SelectorPanel = ({
  timeRange,
  setTimeRange,
  regionFilter,
  setRegionFilter,
}: {
  timeRange: number;
  setTimeRange: (val: number) => void;
  regionFilter: "all" | string;
  setRegionFilter: (val: ChartPanelProps["regionFilter"]) => void;
}) => {
  return (
    <HStack gap={4}>
      <div data-testid="region-select">
        <RegionSelect region={regionFilter} setRegion={setRegionFilter} />
      </div>
      <div data-testid="time-range-select">
        <TimeRangeSelect timeRange={timeRange} setTimeRange={setTimeRange} />
      </div>
    </HStack>
  );
};

const ChartPanel = ({
  dailyCosts,
  regionFilter,
  isLoading,
  isError,
  error,
}: ChartPanelProps) => {
  const totalSpend = useMemo(
    () => getTotalSpend(dailyCosts, regionFilter),
    [dailyCosts, regionFilter],
  );

  return (
    <Box data-testid="chart-panel">
      <HStack justifyContent="space-between" my={3}>
        <Text
          as="h4"
          textStyle="heading-md"
          data-testid={`${regionFilter}-spend-amount`}
        >
          {formatCurrency(totalSpend)}
        </Text>
      </HStack>
      <Flex
        minHeight={legendHeightPx + chartHeightPx}
        data-testid="chart-container"
      >
        {isLoading && (
          <Spinner
            data-testid="chart-loading-spinner"
            size="xl"
            margin="auto"
          />
        )}
        {isError && (
          <ErrorBox
            data-testid="chart-error"
            message={
              error?.message || "There was an error fetching your usage."
            }
          />
        )}
        {!isLoading && !isError && dailyCosts && (
          <DailyUsageChart region={regionFilter} data={dailyCosts} />
        )}
      </Flex>
    </Box>
  );
};

const UsagePage = () => {
  const { organization } = useCurrentOrganization();
  const [regionFilter, setRegionFilter] = useState<"all" | string>("all");
  const [lastQueryTime, setLastQueryTime] = useState(nowUTC());
  const [timeRangeFilter, setTimeRangeFilter] = useState(
    DEFAULT_TIME_RANGE_LOOKBACK_DAYS,
  );
  const {
    data: dailyCosts,
    isLoading: isDailyCostsLoading,
    isError: isDailyCostsError,
    error: dailyCostsError,
  } = useDailyCosts(timeRangeFilter, lastQueryTime);

  const chartTooltipRef = useRef<HTMLDivElement>(null);

  const timeRangeCosts = useMemo(
    () => getTimeRangeSlice(dailyCosts?.daily ?? null, timeRangeFilter),
    [dailyCosts, timeRangeFilter],
  );
  return (
    <>
      <MainContentContainer width="100%" maxWidth="1400px" mx="auto">
        <Grid
          templateAreas={`
            "selects details"
            "chart details"
            "spend details"
            "invoices ."`}
          gridTemplateColumns="minmax(500px, 70%) minmax(300px, 3fr)"
          gridColumnGap={12}
          gridRowGap={10}
        >
          <GridItem area="selects" marginBottom={-8}>
            <SelectorPanel
              timeRange={timeRangeFilter}
              setTimeRange={setTimeRangeFilter}
              regionFilter={regionFilter}
              setRegionFilter={setRegionFilter}
            />
          </GridItem>
          <GridItem area="chart">
            <ChartPanel
              regionFilter={regionFilter}
              setRegionFilter={setRegionFilter}
              timeRange={timeRangeFilter}
              setTimeRange={(range) => {
                setTimeRangeFilter(range);
                setLastQueryTime(nowUTC());
              }}
              dailyCosts={timeRangeCosts}
              isLoading={isDailyCostsLoading}
              isError={isDailyCostsError}
              error={dailyCostsError}
            />
          </GridItem>
          <GridItem area="details">
            <UpgradedPlanDetails
              region={regionFilter}
              dailyCosts={dailyCosts ?? null}
              timeSpan={timeRangeFilter}
            />
          </GridItem>
          <GridItem area="spend">
            <SpendBreakdown
              region={regionFilter}
              dailyCosts={timeRangeCosts}
              totalDays={timeRangeFilter}
            />
          </GridItem>
          <GridItem area="invoices">
            <Text as="h3" textStyle="heading-sm" mb={4}>
              Invoice history
            </Text>
            {organization?.subscription?.marketplace === "aws" ? (
              <AwsMarketplaceInvoiceBanner />
            ) : (
              <InvoiceTableWrapper
                planType={organization?.subscription?.type ?? "uncategorized"}
              />
            )}
          </GridItem>
        </Grid>
      </MainContentContainer>
      <div ref={chartTooltipRef} />
    </>
  );
};

export default UsagePage;
