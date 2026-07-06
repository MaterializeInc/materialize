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
import React, { useRef, useState } from "react";

import { useCurrentOrganization } from "~/api/auth";
import { PlanType } from "~/api/cloudGlobalApi";
import ErrorBox from "~/components/ErrorBox";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { MaterializeTheme } from "~/theme";
import { nowUTC } from "~/util";

import AccountSpendBreakdown from "./AccountSpendBreakdown";
import InvoiceTable from "./InvoiceTable";
import { AccountSpendPlanDetails } from "./PlanDetails";
import { useDailyCostsBreakdown, useRecentInvoices } from "./queries";

const DEFAULT_TIME_RANGE_LOOKBACK_DAYS = 7;
// Fixed window behind the account plan-details box's "Last 30 days" row,
// independent of the time-range selector.
const LAST_30_DAYS_LOOKBACK = 30;

// A leaf account's own invoice list is permanently empty (Orb invoices belong
// only to the billing owner, never split per child), so an empty result here
// means there's nothing to show, ever, not just "still loading." Hide the
// heading along with the table rather than leave a header with no rows.
const InvoiceHistorySection = ({ planType }: { planType: PlanType }) => {
  const { data: invoices, isLoading, isError, error } = useRecentInvoices();
  if (!isLoading && !isError && (invoices?.length ?? 0) === 0) {
    return null;
  }
  return (
    <>
      <Text as="h3" textStyle="heading-sm" mb={4}>
        Invoice history
      </Text>
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

const UsagePage = () => {
  const { organization } = useCurrentOrganization();
  const [regionFilter, setRegionFilter] = useState<"all" | string>("all");
  const [lastQueryTime] = useState(nowUTC());
  const [timeRangeFilter, setTimeRangeFilter] = useState(
    DEFAULT_TIME_RANGE_LOOKBACK_DAYS,
  );
  const {
    data: costBreakdownDays,
    isLoading: isCostBreakdownLoading,
    isError: isCostBreakdownError,
    error: costBreakdownError,
  } = useDailyCostsBreakdown(timeRangeFilter, lastQueryTime);
  // A separate fixed 30-day breakdown feeds the plan-details "Last 30 days" row.
  // When the selected range is also 30 days this shares the query cache.
  const { data: last30BreakdownDays } = useDailyCostsBreakdown(
    LAST_30_DAYS_LOOKBACK,
    lastQueryTime,
  );

  const chartTooltipRef = useRef<HTMLDivElement>(null);

  return (
    <>
      <MainContentContainer width="100%" maxWidth="1400px" mx="auto">
        <Grid
          templateAreas={`
            "breakdown breakdownDetails"
            "invoices ."`}
          gridTemplateColumns="minmax(500px, 70%) minmax(300px, 3fr)"
          gridColumnGap={12}
          gridRowGap={10}
        >
          <GridItem area="breakdown">
            <AccountSpendBreakdown
              days={costBreakdownDays ?? null}
              isLoading={isCostBreakdownLoading}
              isError={isCostBreakdownError}
              error={costBreakdownError}
              regionFilter={regionFilter}
              setRegionFilter={setRegionFilter}
              timeRange={timeRangeFilter}
              setTimeRange={setTimeRangeFilter}
            />
          </GridItem>
          <GridItem area="breakdownDetails">
            <AccountSpendPlanDetails
              days={costBreakdownDays ?? null}
              last30Days={last30BreakdownDays ?? null}
            />
          </GridItem>
          <GridItem area="invoices">
            {organization?.subscription?.marketplace === "aws" ? (
              <>
                <Text as="h3" textStyle="heading-sm" mb={4}>
                  Invoice history
                </Text>
                <AwsMarketplaceInvoiceBanner />
              </>
            ) : (
              <InvoiceHistorySection
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
