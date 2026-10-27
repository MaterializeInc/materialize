// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Divider, HStack, useTheme } from "@chakra-ui/react";
import React from "react";

import {
  LicenseInformationContent,
  LicenseKeyCard,
  LicenseKeyCTAContent,
  NoLicenseKeyEmptyState,
} from "~/components/licenseComponents";
import { MaterializeTheme } from "~/theme";

import { useLicenseKey } from "./queries";

export const SelfManagedLicenseInformation = () => {
  const { data, isLoading, isError } = useLicenseKey();

  const licenseKey = data?.rows?.at(0);

  // Handle case where we have data but no license key
  if (!isLoading && !isError && !licenseKey) {
    return <NoLicenseKeyEmptyState />;
  }

  const { organization, expiration } = licenseKey || {};

  // For community license, organization is the email address of the user but uuid for enterprise
  const isCommunity = organization?.includes("@");

  return (
    <>
      {isCommunity ? (
        <CommunityLicenseInformation
          expiration={expiration}
          isLoading={isLoading}
          isError={isError}
        />
      ) : (
        <EnterpriseSelfManagedLicenseInformation
          expiration={expiration}
          isLoading={isLoading}
          isError={isError}
        />
      )}
    </>
  );
};

const CommunityLicenseInformation = ({
  expiration,
  isLoading,
  isError,
}: {
  expiration?: Date;
  isLoading: boolean;
  isError: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <LicenseKeyCard
      loading={isLoading}
      isError={isError}
      containerProps={{
        maxWidth: "none",
      }}
    >
      <HStack
        align="stretch"
        gap={6}
        overflow="hidden"
        direction={{ base: "column", lg: "row" }}
      >
        <Box
          flexShrink="0"
          minWidth={{ base: "100%", lg: "320px" }}
          maxWidth={{ base: "100%", lg: "400px" }}
        >
          <LicenseInformationContent
            canIssueLicense={false}
            expirationDate={expiration}
            limits="24 GB RAM"
            licenseType="community"
          />
        </Box>
        <Divider orientation="vertical" borderColor={colors.border.primary} />
        <LicenseKeyCTAContent selfManagedMode="community" />
      </HStack>
    </LicenseKeyCard>
  );
};

const EnterpriseSelfManagedLicenseInformation = ({
  expiration,
  isLoading,
  isError,
}: {
  expiration?: Date;
  isLoading: boolean;
  isError: boolean;
}) => {
  return (
    <LicenseKeyCard loading={isLoading} isError={isError}>
      <LicenseInformationContent
        expirationDate={expiration}
        limits="Unlimited"
        licenseType="enterprise"
      />
    </LicenseKeyCard>
  );
};
