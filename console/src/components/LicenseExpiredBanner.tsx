// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CloseButton, Flex, Text } from "@chakra-ui/react";
import * as React from "react";
import { Link as RouterLink } from "react-router-dom";

import { useLicenseKey } from "~/access/license/queries";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import { useIsSuperUser } from "~/hooks/useIsSuperUser";
import useLocalStorage from "~/hooks/useLocalStorage";

import { AlertBanner } from "./Alert";
import SupportLink from "./SupportLink";
import TextLink from "./TextLink";

/** Stores the epoch milliseconds of the most recent dismissal, 0 if never. */
export const LICENSE_EXPIRED_DISMISSED_AT_KEY =
  "mz-license-expired-dismissed-at";

/** How long a dismissal lasts before the banner shows again. */
export const DISMISSAL_DURATION_MS = 7 * 24 * 60 * 60 * 1000;

const LicenseExpiredBannerContent = () => {
  const { isSuperUser } = useIsSuperUser();
  const { data } = useLicenseKey();

  const [dismissedAt, setDismissedAt] = useLocalStorage<number>(
    LICENSE_EXPIRED_DISMISSED_AT_KEY,
    0,
  );
  const dismissed = Date.now() - dismissedAt < DISMISSAL_DURATION_MS;

  const licenseKey = data?.rows?.at(0);
  const { expiration, organization } = licenseKey ?? {};
  // Community keys carry the requester's email in the organization field,
  // enterprise keys a UUID. Matches SelfManagedLicenseInformation.tsx.
  const isCommunity = organization?.includes("@");
  // Canonical expiry check, matching licenseComponents.tsx (isActive).
  const expired = expiration && new Date() > new Date(expiration);

  if (
    // Only show this banner to super users
    !isSuperUser ||
    // Don't show it until we know the license has actually expired. This also
    // covers the loading state and the no-license-key case, so it doesn't flash.
    !expired ||
    dismissed
  ) {
    return null;
  }

  return (
    <AlertBanner
      variant="error"
      data-testid="license-expired-alert"
      flexShrink="0"
    >
      <Flex justifyContent="center" width="100%">
        {isCommunity ? (
          <Text>
            Your Materialize Community license has expired. Generate a new key
            on the{" "}
            <TextLink as={RouterLink} to="/license">
              license page
            </TextLink>
            .
          </Text>
        ) : (
          <Text>
            Your Materialize Enterprise license has expired.{" "}
            <SupportLink>Contact support</SupportLink> to renew.
          </Text>
        )}
      </Flex>
      <CloseButton
        position="relative"
        right="0"
        size="sm"
        onClick={() => {
          setDismissedAt(Date.now());
        }}
      />
    </AlertBanner>
  );
};

/**
 * A full-width banner shown to self-managed super users when the environment's
 * license has expired. Community licenses point at the license page for
 * self-service renewal, enterprise licenses at support. Dismissible, with the
 * dismissal persisted in local storage for a week, after which the banner
 * shows again.
 */
export const LicenseExpiredBanner = () => (
  <AppConfigSwitch selfManagedConfigElement={<LicenseExpiredBannerContent />} />
);
