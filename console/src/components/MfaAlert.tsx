// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CloseButton, Flex, Text } from "@chakra-ui/react";
import { addDays } from "date-fns";
import * as React from "react";

import { isSuperUser } from "~/api/auth";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import {
  AdminPortal,
  User,
  UserManagedByEnum,
} from "~/external-library-wrappers/frontegg";
import useLocalStorage from "~/hooks/useLocalStorage";
import { useMfaPolicy } from "~/queries/frontegg";

import { AlertBanner } from "./Alert";
import TextLink from "./TextLink";

export const MFA_DISMISSED_KEY = "mz-mfa-alert-dismissed";

const MfaAlertContent = ({ user }: { user: User }) => {
  const { data: mfaPolicy } = useMfaPolicy();

  const [dismissedAt, setDismissedAt] = useLocalStorage<Date | null>(
    MFA_DISMISSED_KEY,
    null,
  );

  if (
    // Only show this banner to org admins
    !isSuperUser(user) ||
    // Don't prompt them to enable MFA if they're using SSO
    user.managedBy !== UserManagedByEnum.FRONTEGG ||
    mfaPolicy === undefined ||
    mfaPolicy.enforceMFAType === "Force" ||
    mfaPolicy.enforceMFAType === "ForceExceptSAML" ||
    (dismissedAt && new Date() < addDays(dismissedAt, 30))
  ) {
    return null;
  }

  return (
    <AlertBanner variant="info" data-testid="mfa-required-alert" flexShrink="0">
      <Flex justifyContent="center" width="100%">
        <Text>
          Please{" "}
          <TextLink
            href={`${location.href}#/admin-box/security/mfa`}
            onClick={(e) => {
              e.preventDefault();
              location.hash = "/admin-box/security/mfa";
              AdminPortal.show();
            }}
          >
            require MFA
          </TextLink>{" "}
          for your organization.
        </Text>
      </Flex>
      <CloseButton
        position="relative"
        right="0"
        size="sm"
        onClick={() => {
          setDismissedAt(new Date());
        }}
      />
    </AlertBanner>
  );
};

export const MfaAlert = () => (
  <AppConfigSwitch
    cloudConfigElement={({ runtimeConfig }) =>
      runtimeConfig.isImpersonating ? null : (
        <MfaAlertContent user={runtimeConfig.user} />
      )
    }
  />
);
