// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import { Stripe } from "@stripe/stripe-js";
import React from "react";
import { Navigate, Outlet, Route } from "react-router-dom";

import { useCanViewBilling, useCurrentOrganization } from "~/api/auth";
import { SupportButton } from "~/components/SupportLink";
import { CloudRuntimeConfig } from "~/config/AppConfigSwitch";
import { PageHeader, PageTab, PageTabStrip, Tab } from "~/layouts/BaseLayout";
import { SentryRoutes } from "~/sentry";
import { MaterializeTheme } from "~/theme";

import BillingPage from "./BillingPage";
import PricingPage from "./PricingPage";
import { useRecentInvoices } from "./queries";
import UsagePage from "./UsagePage";
import { getIsUpgradedPlan } from "./utils";

const UsageLayout = ({ isBillingVisible }: { isBillingVisible: boolean }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { organization } = useCurrentOrganization();
  const isUpgradedPlan = getIsUpgradedPlan(organization?.subscription?.type);

  const navItems: Tab[] = React.useMemo(
    () => [
      // Hide overview page for trial users given the page has no useful information for them.
      ...(isUpgradedPlan
        ? [{ label: "Overview", href: "/usage/overview", end: true }]
        : []),
      ...(isBillingVisible
        ? [{ label: "Billing", href: "/usage/billing", end: true }]
        : []),
    ],
    [isBillingVisible, isUpgradedPlan],
  );
  return (
    <>
      <PageHeader variant="compact" sticky>
        <VStack spacing={0} alignItems="flex-start" width="100%">
          <HStack
            borderBottom="solid 1px"
            borderColor={colors.border.primary}
            justifyContent="space-between"
            width="100%"
            py="4"
            px="7"
          >
            <Text textStyle="heading-md">Usage &amp; Billing</Text>
            <SupportButton>Contact support</SupportButton>
          </HStack>

          <HStack width="100%" spacing="3" pr="7">
            <Box flex="1" minWidth="0">
              <PageTabStrip tabData={navItems} />
            </Box>
            {/* Static reference material, not account-specific data, so it
                lives here rather than behind the (root-only) Billing tab.
                Styled as a PageTab (not a Button) to read as another pill in
                the strip rather than an unrelated bordered button. */}
            <PageTab
              to="/usage/pricing"
              tabProps={{
                flexShrink: 0,
                borderRadius: "4px",
                _hover: { background: colors.background.secondary },
              }}
            >
              Pricing
            </PageTab>
          </HStack>
        </VStack>
      </PageHeader>
      <Outlet />
    </>
  );
};

const UsageRoutes = ({
  runtimeConfig,
  stripePromise,
}: {
  runtimeConfig: CloudRuntimeConfig;
  stripePromise: Promise<Stripe | null>;
}) => {
  const { organization } = useCurrentOrganization();

  const isUpgradedPlan = getIsUpgradedPlan(organization?.subscription?.type);
  const { data: invoices } = useRecentInvoices();
  // Proxy for "is this a leaf account": a leaf's invoices always read as
  // empty (SAS-148), same signal InvoiceHistorySection uses. Evaluation orgs
  // are also invoice-less pre-upgrade, so carve those out to keep Billing
  // reachable for a first-time "Upgrade & Pay". Known gap and the direct fix
  // (an is_billing_child field, already written but undeployed): SAS-150.
  const isBillingVisible =
    useCanViewBilling({ runtimeConfig }) &&
    (organization?.subscription?.type === "evaluation" ||
      (invoices?.length ?? 0) > 0);

  const shouldRedirectToBilling = isBillingVisible && !isUpgradedPlan;

  return (
    <SentryRoutes>
      <Route element={<UsageLayout isBillingVisible={isBillingVisible} />}>
        {isUpgradedPlan && <Route path="/overview" element={<UsagePage />} />}
        {isBillingVisible && !runtimeConfig.isImpersonating && (
          <Route
            path="/billing"
            element={
              <BillingPage
                user={runtimeConfig.user}
                stripePromise={stripePromise}
              />
            }
          />
        )}
        {/* Static reference material, not account-specific, so it's reachable
            regardless of plan type or billing-child status. */}
        <Route path="/pricing" element={<PricingPage />} />

        <Route
          index
          element={
            <Navigate
              to={
                shouldRedirectToBilling ? "/usage/billing" : "/usage/overview"
              }
              replace
            />
          }
        />
      </Route>
    </SentryRoutes>
  );
};

export default UsageRoutes;
