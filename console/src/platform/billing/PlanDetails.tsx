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
  ButtonProps,
  Collapse,
  Divider,
  Heading,
  HStack,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { PropsWithChildren, useMemo } from "react";

import { useCurrentOrganization } from "~/api/auth";
import { CostBreakdownDay } from "~/api/cloudGlobalApi";
import ScheduleDemoLink from "~/components/ScheduleDemoLink";
import SupportLink from "~/components/SupportLink";
import LowerLeftCornerIcon from "~/svg/LowerLeftCornerIcon";
import { MaterializeTheme } from "~/theme";
import { formatDateInUtc, FRIENDLY_DATE_FORMAT } from "~/utils/dateFormat";
import { formatCurrency } from "~/utils/format";

import {
  accountTotal,
  aggregateDays,
  breakdownByAccount,
  filterDaysByRegion,
  shortAccountId,
} from "./dailyBreakdown";
import { useCreditBalance } from "./queries";
import { calculateNextOnDemandPaymentDate } from "./utils";

const planTypeDisplayNames = {
  partner: "Partner",
  capacity: "Capacity",
  internal: "Internal",
  "on-demand": "On Demand",
  evaluation: "Evaluation",
  uncategorized: "Unknown",
} as const;

const PlanSectionHeader = ({
  title,
  value,
}: {
  title: string;
  value: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack justifyContent="space-between" px="4">
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        {title}
      </Text>
      <Text textStyle="text-ui-med">{value}</Text>
    </HStack>
  );
};

const PlanSectionItem = ({
  title,
  value,
}: {
  title: string;
  value: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack justifyContent="space-between" px="4" mt="2">
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        <LowerLeftCornerIcon color={colors.border.secondary} />
        {title}
      </Text>
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        {value}
      </Text>
    </HStack>
  );
};

const PlanSection = ({
  children,
  showDivider,
}: PropsWithChildren<{ showDivider?: boolean }>) => {
  return (
    <>
      {children}
      {(showDivider ?? true) && <Divider my="3" />}
    </>
  );
};

const PlanDetailsContainer = ({
  children,
  testId = "plan-details",
}: PropsWithChildren<{ testId?: string }>) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Box
      data-testid={testId}
      borderRadius="lg"
      border="1px solid"
      borderColor={colors.border.primary}
      backgroundColor={colors.background.primary}
      overflow="hidden"
    >
      {children}
    </Box>
  );
};

const ContactUsContainer = ({ children }: PropsWithChildren) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Box backgroundColor={colors.background.secondary} padding="4" mt="-3">
      {children}
    </Box>
  );
};

export const UpgradedPlanDetails = () => {
  const { organization } = useCurrentOrganization();
  const { colors } = useTheme<MaterializeTheme>();
  const { data: creditBalance } = useCreditBalance();

  return (
    <PlanDetailsContainer>
      <Heading as="h3" fontSize="sm" fontWeight="500" px="4" py="3">
        Plan details
      </Heading>
      <VStack alignItems="stretch" mt="3" gap={0}>
        {/* If on demand, show next billing date. */}
        <PlanSection>
          <PlanSectionHeader
            title="Plan type"
            value={
              organization?.subscription
                ? planTypeDisplayNames[organization.subscription.type]
                : "-"
            }
          />
        </PlanSection>
        <Collapse in={organization?.subscription?.type === "capacity"}>
          <PlanSection>
            <PlanSectionHeader
              title="Total balance"
              value={formatCurrency(creditBalance ?? 0)}
            />
          </PlanSection>
        </Collapse>
      </VStack>
      <Collapse in={organization?.subscription?.type === "on-demand"}>
        <PlanSection>
          <PlanSectionHeader
            title="Next payment date"
            value={formatDateInUtc(
              calculateNextOnDemandPaymentDate(),
              FRIENDLY_DATE_FORMAT,
            )}
          />
        </PlanSection>
      </Collapse>
      {organization?.subscription?.type === "capacity" ? (
        <ContactUsContainer>
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            Getting towards the end of your balance?
          </Text>
          <ScheduleDemoLink fontWeight="500" textStyle="text-small">
            Talk to our team &#x027F6;
          </ScheduleDemoLink>
        </ContactUsContainer>
      ) : organization?.subscription?.type === "on-demand" ? (
        <ContactUsContainer>
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            Looking to cancel your subscription?
          </Text>
          <SupportLink fontWeight="500" textStyle="text-small">
            Contact Support &#x027F6;
          </SupportLink>
        </ContactUsContainer>
      ) : (
        <Box paddingBottom="4" />
      )}
    </PlanDetailsContainer>
  );
};

/**
 * Sum every account's cost across a per-account daily breakdown window.
 * `null` if there's no data to show, e.g. an empty window or a region filter
 * that leaves no accounts (matches the ledger's own "no usage" empty state
 * rather than rendering a misleading $0.00).
 */
function breakdownTotal(days: CostBreakdownDay[] | null): number | null {
  if (!days || days.length === 0) {
    return null;
  }
  const accounts = aggregateDays(days).accounts;
  if (accounts.length === 0) {
    return null;
  }
  return accounts.reduce((sum, account) => sum + accountTotal(account), 0);
}

/**
 * Plan-details box for the Direction-A account-spend section. Mirrors
 * `UpgradedPlanDetails`, but sources spend from the per-account daily breakdown
 * (`/api/costs/breakdown/daily`) rather than `/api/costs/daily`, so it carries
 * no dependency on the legacy daily-costs endpoint. Plan type and balance come
 * from the organization and credits, so the box still renders while the
 * breakdown is loading or empty. `days` is the selected window (drives total
 * spend and daily average); `last30Days` is a fixed 30-day window. Both are
 * restricted to `regionFilter` so the totals here match the ledger.
 */
export const AccountSpendPlanDetails = ({
  days,
  last30Days,
  regionFilter,
}: {
  days: CostBreakdownDay[] | null;
  last30Days: CostBreakdownDay[] | null;
  regionFilter: "all" | string;
}) => {
  const { organization } = useCurrentOrganization();
  const { colors } = useTheme<MaterializeTheme>();
  const { data: creditBalance } = useCreditBalance();

  const filteredDays = useMemo(
    () => (days ? filterDaysByRegion(days, regionFilter) : null),
    [days, regionFilter],
  );
  const filteredLast30Days = useMemo(
    () => (last30Days ? filterDaysByRegion(last30Days, regionFilter) : null),
    [last30Days, regionFilter],
  );
  const total = useMemo(() => breakdownTotal(filteredDays), [filteredDays]);
  // The window is dense (one bucket per UTC day), so `days.length` is the day
  // count the average divides by.
  const dailyAverage =
    total !== null && filteredDays ? total / filteredDays.length : null;
  const last30 = useMemo(
    () => breakdownByAccount(filteredLast30Days),
    [filteredLast30Days],
  );

  return (
    <PlanDetailsContainer testId="account-plan-details">
      <Heading as="h3" fontSize="sm" fontWeight="500" px="4" py="3">
        Plan details
      </Heading>
      <VStack alignItems="stretch" mt="3" gap={0}>
        <PlanSection>
          <PlanSectionHeader
            title="Plan type"
            value={
              organization?.subscription
                ? planTypeDisplayNames[organization.subscription.type]
                : "-"
            }
          />
        </PlanSection>
        <Collapse in={organization?.subscription?.type === "capacity"}>
          <PlanSection>
            <PlanSectionHeader
              title="Total balance"
              value={formatCurrency(creditBalance ?? 0)}
            />
          </PlanSection>
        </Collapse>
        {total !== null && (
          <PlanSection>
            <PlanSectionHeader
              title="Total spend"
              value={formatCurrency(total)}
            />
          </PlanSection>
        )}
        {last30 && (
          <PlanSection>
            <PlanSectionHeader
              title="Last 30 days"
              value={formatCurrency(last30.total)}
            />
            {last30.accounts.length > 1 &&
              last30.accounts.map((account) => (
                <PlanSectionItem
                  key={account.id}
                  title={account.name || shortAccountId(account.id)}
                  value={formatCurrency(account.total)}
                />
              ))}
          </PlanSection>
        )}
        {dailyAverage !== null && (
          <PlanSection>
            <PlanSectionHeader
              title="Daily average"
              value={formatCurrency(dailyAverage)}
            />
          </PlanSection>
        )}
      </VStack>
      <Collapse in={organization?.subscription?.type === "on-demand"}>
        <PlanSection>
          <PlanSectionHeader
            title="Next payment date"
            value={formatDateInUtc(
              calculateNextOnDemandPaymentDate(),
              FRIENDLY_DATE_FORMAT,
            )}
          />
        </PlanSection>
      </Collapse>
      <ContactUsContainer>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          Looking to cancel your subscription?
        </Text>
        <SupportLink fontWeight="500" textStyle="text-small">
          Contact Support &#x027F6;
        </SupportLink>
      </ContactUsContainer>
    </PlanDetailsContainer>
  );
};

export const EvaluationPlanDetails = ({
  upgradeButtonProps,
  canManagePayments = true,
}: {
  upgradeButtonProps?: ButtonProps;
  canManagePayments?: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <PlanDetailsContainer>
      <Heading as="h3" fontSize="sm" fontWeight="500" px="4" py="3">
        Plan details
      </Heading>
      <VStack alignItems="stretch" mt="3" gap={0}>
        <PlanSection showDivider={false}>
          <PlanSectionHeader title="Plan type" value="Evaluation" />
        </PlanSection>
      </VStack>
      <VStack width="100%" gap={0} alignItems="stretch" px="4" py="3">
        {canManagePayments ? (
          <Button variant="primary" size="sm" {...upgradeButtonProps}>
            Upgrade &amp; Pay
          </Button>
        ) : (
          <Text
            textStyle="text-small"
            color={colors.foreground.secondary}
            textAlign="center"
            as="i"
          >
            Contact an organization admin to upgrade.
          </Text>
        )}
      </VStack>
    </PlanDetailsContainer>
  );
};
