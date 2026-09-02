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
import { PageHeader, PageTabStrip, Tab } from "~/layouts/BaseLayout";
import { SentryRoutes } from "~/sentry";
import { MaterializeTheme } from "~/theme";

import BillingPage from "./BillingPage";
import PricingPage from "./PricingPage";
import { useRecentInvoices } from "./queries";
import UsagePage from "./UsagePage";
import { getIsUpgradedPlan } from "./utils";

// Static reference material, not account-specific data, so it lives to the
// right of the account tabs rather than behind the (root-only) Billing tab.
const PRICING_TAB: Tab[] = [
  { label: "Pricing", href: "/usage/pricing", end: true },
];

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

          {/* Each PageTabStrip draws its underline only across its own width,
              which would leave gaps in the strips' spacing and the row's
              right padding. The same 1px inset line on the row itself keeps
              it continuous. Bottom-align the strips (the active tab's 1px
              borderBottom makes its strip 1px taller; centering would float
              the other strip's underline off the row's) so all the underline
              segments land on the same pixel row. */}
          <HStack
            width="100%"
            spacing="3"
            pr="7"
            alignItems="flex-end"
            boxShadow={`inset 0px -1px 0px 0px ${colors.border.primary}`}
          >
            <Box flex="1" minWidth="0">
              <PageTabStrip tabData={navItems} />
            </Box>
            {/* A separate single-tab strip so Pricing shares the tab strip's
                hover pill and active underline instead of hand-rolled hover
                styling. */}
            <Box flexShrink={0}>
              <PageTabStrip tabData={PRICING_TAB} />
            </Box>
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
  // NOTE: this reads as "hide" while the query is loading, briefly hiding
  // Billing/the route for a real billing owner on first load. Don't "fix"
  // that by failing open on loading/error: the route below is gated on this
  // same flag, and failing open can flip true->false for a leaf account
  // already on /usage/billing, unmounting the route with no fallback and no
  // way back. This direction (hidden -> visible, self-correcting) is safer.
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
