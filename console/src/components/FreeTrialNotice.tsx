// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, BoxProps, Button, Text, useTheme } from "@chakra-ui/react";
import { differenceInDays } from "date-fns";
import React from "react";
import { Link } from "react-router-dom";

import {
  hasEnvironmentWritePermission,
  useCurrentOrganization,
} from "~/api/auth";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import { User } from "~/external-library-wrappers/frontegg";
import { useFlags } from "~/hooks/useFlags";
import { InfoIcon } from "~/icons";
import docUrls from "~/mz-doc-urls.json";
import { MaterializeTheme } from "~/theme";

import ScheduleDemoLink from "./ScheduleDemoLink";

const FreeTrialNoticeContent = ({
  user,
  ...props
}: BoxProps & { user: User }) => {
  const { colors } = useTheme<MaterializeTheme>();

  const { organization } = useCurrentOrganization();

  const flags = useFlags();

  const isOrgAdmin = hasEnvironmentWritePermission(user);

  const isBillingEnabled = flags["billing-ui-3756"];

  if (
    !organization ||
    organization.subscription?.type !== "evaluation" ||
    !organization.trialExpiresAt
  ) {
    return null;
  }

  const trialExpiresAt = new Date(organization.trialExpiresAt);
  const now = new Date();
  const daysRemaining = differenceInDays(trialExpiresAt, now);
  const expired = trialExpiresAt <= now;
  return (
    <Box
      borderWidth="1px"
      borderColor={colors.border.info}
      borderRadius="8"
      background={colors.background.info}
      p="4"
      display={{ base: "none", lg: "block" }}
      {...props}
    >
      <Text
        display="flex"
        textStyle="text-small"
        color={colors.foreground.secondary}
      >
        Free trial
        <Box
          as="a"
          href={docUrls["/docs/free-trial-faqs/"]}
          ml="1"
          rel="noopener"
          target="_blank"
        >
          <InfoIcon />
        </Box>
      </Text>
      <Text mt="1" textStyle="heading-sm">
        {expired
          ? "Trial Expired"
          : `${daysRemaining} ${
              daysRemaining === 1 ? "day" : "days"
            } remaining`}
      </Text>

      <Text textStyle="text-small" color={colors.foreground.secondary} mt="2">
        Have questions? <ScheduleDemoLink>Talk to our team</ScheduleDemoLink>.
      </Text>

      {isBillingEnabled && isOrgAdmin && (
        <Button
          mt="4"
          size="sm"
          variant="primary"
          width="100%"
          as={Link}
          to="/usage/billing"
        >
          Upgrade Plan
        </Button>
      )}
    </Box>
  );
};

const FreeTrialNotice = (props: BoxProps) => {
  return (
    <AppConfigSwitch
      cloudConfigElement={({ runtimeConfig }) =>
        runtimeConfig.isImpersonating ? null : (
          <FreeTrialNoticeContent {...props} user={runtimeConfig.user} />
        )
      }
    />
  );
};

export default FreeTrialNotice;
