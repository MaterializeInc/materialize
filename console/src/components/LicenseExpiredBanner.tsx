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

import { useLicenseKey } from "~/access/license/queries";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import { useIsSuperUser } from "~/hooks/useIsSuperUser";
import useLocalStorage from "~/hooks/useLocalStorage";

import { AlertBanner } from "./Alert";
import TextLink from "./TextLink";

export const LICENSE_EXPIRED_DISMISSED_KEY = "mz-license-expired-dismissed";

/**
 * Support URL used by the License page ("Questions about your license? Talk to us").
 * Reused here so the banner points super users to the same place.
 */
const SUPPORT_URL = "https://materialize.com/s/chat";

const LicenseExpiredBannerContent = () => {
  const { isSuperUser } = useIsSuperUser();
  const { data } = useLicenseKey();

  const [dismissed, setDismissed] = useLocalStorage<boolean>(
    LICENSE_EXPIRED_DISMISSED_KEY,
    false,
  );

  const licenseKey = data?.rows?.at(0);
  const { expiration } = licenseKey ?? {};
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
        <Text>
          Your Materialize Enterprise license has expired.{" "}
          <TextLink as="a" href={SUPPORT_URL} target="_blank" rel="noreferrer">
            Contact support
          </TextLink>{" "}
          to renew.
        </Text>
      </Flex>
      <CloseButton
        position="relative"
        right="0"
        size="sm"
        onClick={() => {
          setDismissed(true);
        }}
      />
    </AlertBanner>
  );
};

/**
 * A full-width banner shown to self-managed super users when the environment's
 * enterprise license has expired. Dismissible, with the dismissal persisted in
 * local storage.
 */
export const LicenseExpiredBanner = () => (
  <AppConfigSwitch selfManagedConfigElement={<LicenseExpiredBannerContent />} />
);
