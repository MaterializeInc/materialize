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
import { useAtom } from "jotai";
import React, { PropsWithChildren, useMemo } from "react";

import { useCurrentOrganization } from "~/api/auth";
import { DailyCosts } from "~/api/cloudGlobalApi";
import ScheduleDemoLink from "~/components/ScheduleDemoLink";
import SupportLink from "~/components/SupportLink";
import { cloudRegionsSelector } from "~/store/cloudRegions";
import LowerLeftCornerIcon from "~/svg/LowerLeftCornerIcon";
import { MaterializeTheme } from "~/theme";
import { formatDateInUtc, FRIENDLY_DATE_FORMAT } from "~/utils/dateFormat";
import { formatCurrency } from "~/utils/format";

import { useCreditBalance } from "./queries";
import { calculateNextOnDemandPaymentDate, summarizePlanCosts } from "./utils";

type UpgradedPlanDetailsProps = {
  region: "all" | string;
  dailyCosts: DailyCosts | null;
  timeSpan: number | null;
};

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

const PlanDetailsContainer = ({ children }: PropsWithChildren) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Box
      data-testid="plan-details"
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

export const UpgradedPlanDetails = ({
  region,
  dailyCosts,
  timeSpan,
}: UpgradedPlanDetailsProps) => {
  const { organization } = useCurrentOrganization();
  const { colors } = useTheme<MaterializeTheme>();
  const { data: creditBalance } = useCreditBalance();
  const [cloudRegions] = useAtom(cloudRegionsSelector);

  const { spanSummary, last30Summary } = useMemo(() => {
    if (!timeSpan) {
      return {
        spanSummary: null,
        last30Summary: null,
      };
    }
    return summarizePlanCosts(
      dailyCosts ? dailyCosts.daily : null,
      timeSpan,
      cloudRegions,
    );
  }, [dailyCosts, timeSpan, cloudRegions]);
  const isRegionFiltered = region !== "all";
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
        {last30Summary && (
          <PlanSection>
            <PlanSectionHeader
              title="Last 30 days"
              value={formatCurrency(
                isRegionFiltered
                  ? (last30Summary.regions.get(region) ?? 0)
                  : last30Summary.total,
              )}
            />
            <Collapse in={!isRegionFiltered}>
              {[...last30Summary.regions.entries()].map(
                ([regionId, regionTotal]) => (
                  <PlanSectionItem
                    key={regionId}
                    title={regionId}
                    value={formatCurrency(regionTotal)}
                  />
                ),
              )}
            </Collapse>
          </PlanSection>
        )}
        {spanSummary && timeSpan && (
          <PlanSection showDivider>
            <PlanSectionHeader
              title="Daily average"
              value={formatCurrency(
                (isRegionFiltered
                  ? (spanSummary.regions.get(region) ?? 0)
                  : spanSummary.total) / timeSpan,
              )}
            />
            <Collapse in={!isRegionFiltered}>
              {[...spanSummary.regions.entries()].map(
                ([regionId, regionTotal]) => (
                  <PlanSectionItem
                    key={regionId}
                    title={regionId}
                    value={formatCurrency(regionTotal / timeSpan)}
                  />
                ),
              )}
            </Collapse>
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

export const EvaluationPlanDetails = ({
  upgradeButtonProps,
}: {
  upgradeButtonProps?: ButtonProps;
}) => {
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
        <Button variant="primary" size="sm" {...upgradeButtonProps}>
          Upgrade &amp; Pay
        </Button>
      </VStack>
    </PlanDetailsContainer>
  );
};
