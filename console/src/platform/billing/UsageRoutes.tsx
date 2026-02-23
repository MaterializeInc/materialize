// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Text, useTheme, VStack } from "@chakra-ui/react";
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

          <PageTabStrip tabData={navItems} />
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
  const isBillingVisible = useCanViewBilling({ runtimeConfig });

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
