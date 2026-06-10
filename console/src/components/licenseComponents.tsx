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
  Card,
  Circle,
  Flex,
  HStack,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { ComponentProps, useState } from "react";

import { useSegment } from "~/analytics/segment";
import Alert from "~/components/Alert";
import { SecretCopyableBox } from "~/components/copyableComponents";
import TextLink from "~/components/TextLink";
import {
  EmptyListHeader,
  EmptyListHeaderContents,
  EmptyListWrapper,
} from "~/layouts/listPageComponents";
import docUrls from "~/mz-doc-urls.json";
import {
  useIssueCommunityLicenseKey,
  useIssueLicenseKey,
} from "~/queries/cloudGlobalApi";
import ActivityIcon from "~/svg/ActivityIcon";
import { ArrowRightIcon } from "~/svg/ArrowRightIcon";
import BoxIcon from "~/svg/BoxIcon";
import { MaterializeTheme } from "~/theme";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import StatusPill from "./StatusPill";

type SelfManagedMode = "enterprise" | "community";

export interface LicenseKeyCardProps {
  loading?: boolean;
  isError?: boolean;
  children?: React.ReactNode;
  containerProps?: ComponentProps<typeof Card>;
}

export const LicenseKeyCard = ({
  loading,
  isError,
  children,
  containerProps,
}: LicenseKeyCardProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Card
      p={6}
      width="100%"
      maxWidth="600px"
      borderRadius="md"
      border="1px"
      borderColor={colors.border.primary}
      {...containerProps}
    >
      {isError ? (
        <Alert
          variant="error"
          message="Failed to load license information. Please try refreshing the page."
          width="100%"
        />
      ) : loading ? (
        <Flex justify="center" align="center" minHeight="200px">
          <Spinner size="lg" color={colors.accent.brightPurple} />
        </Flex>
      ) : (
        children
      )}
    </Card>
  );
};

export const GenerateNewLicenseKey = ({
  email = "",
  licenseType = "enterprise",
}: {
  email?: string;
  licenseType: "community" | "enterprise";
}) => {
  const [newLicenseKey, setNewLicenseKey] = useState<{
    key: string;
    obfuscated: string;
  } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const { colors } = useTheme<MaterializeTheme>();

  const enterpriseMutation = useIssueLicenseKey();
  const communityMutation = useIssueCommunityLicenseKey();

  const handleGenerateNewKey = () => {
    setError(null);
    const onSuccess = (data: any) => {
      if (data?.licenseKey) {
        const obfuscated = new Array(data.licenseKey.length).fill("*").join("");
        setNewLicenseKey({
          key: data.licenseKey,
          obfuscated,
        });
      }
    };

    const onError = (err: unknown) => {
      console.error("Failed to generate license key:", err);
      setError("Failed to generate license key");
    };

    if (licenseType === "community") {
      communityMutation.mutate(email, { onSuccess, onError });
    } else {
      enterpriseMutation.mutate(undefined, { onSuccess, onError });
    }
  };

  const issueLicenseKeyMutation =
    licenseType === "community" ? communityMutation : enterpriseMutation;

  return (
    <VStack
      align="start"
      spacing={3}
      width="100%"
      bg={colors.background.secondary}
      p={4}
      borderRadius="md"
    >
      {!newLicenseKey ? (
        <VStack align="start" spacing={2} width="100%">
          <HStack
            width="100%"
            justifyContent="space-between"
            alignItems="center"
            spacing={4}
          >
            <Box flex="1">
              <Text
                fontSize="sm"
                fontWeight="500"
                color={colors.foreground.secondary}
              >
                {licenseType === "community"
                  ? "Generate a new community license key for your self managed installation"
                  : "Generate a new license key for your self managed installation"}
              </Text>
            </Box>
            <Button
              onClick={handleGenerateNewKey}
              isLoading={issueLicenseKeyMutation.isPending}
              isDisabled={issueLicenseKeyMutation.isPending}
              variant="primary"
              size="md"
              loadingText="Generating..."
              spinner={<Spinner size="sm" />}
              flexShrink={0}
            >
              Generate new key
            </Button>
          </HStack>
          {error && (
            <Text fontSize="sm" color={colors.accent.red}>
              {error}
            </Text>
          )}
        </VStack>
      ) : (
        <VStack alignItems="stretch" width="100%">
          <Text
            fontSize="sm"
            fontWeight="500"
            color={colors.foreground.primary}
          >
            New license key
          </Text>
          <SecretCopyableBox
            label="License Key"
            contents={newLicenseKey.key}
            obfuscatedContent={newLicenseKey.obfuscated}
            bg={colors.background.primary}
          />
          <Text fontSize="sm" color={colors.foreground.secondary}>
            Copy this license key to somewhere safe. License keys cannot be
            displayed after initial creation.
          </Text>
        </VStack>
      )}
    </VStack>
  );
};

export interface LicenseInformationContentProps {
  email?: string;
  canIssueLicense?: boolean;
  expirationDate: Date | string | undefined;
  limits: string;
  licenseType?: "community" | "enterprise";
}

export const LicenseInformationContent = ({
  email,
  canIssueLicense,
  expirationDate,
  limits,
  licenseType = "enterprise",
}: LicenseInformationContentProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const formatExpirationDate = () => {
    if (!expirationDate) return "Not available";
    return formatDate(
      new Date(expirationDate),
      FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
    );
  };

  const isActive = expirationDate && new Date() <= new Date(expirationDate);
  return (
    <VStack align="start" spacing={4}>
      <Text textStyle="heading-lg">License Information</Text>
      {canIssueLicense && <GenerateNewLicenseKey licenseType="enterprise" />}
      <VStack align="start" spacing={3} width="100%">
        <HStack justify="space-between" width="100%">
          <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
            Status
          </Text>
          <StatusPill
            status={isActive ? "active" : "expired"}
            colorScheme={isActive ? "green" : "red"}
            label={isActive ? "Active" : "Expired"}
            paddingY="1"
            paddingLeft="2"
            paddingRight="3"
            textStyle="text-ui-med"
            borderRadius="10"
            textAlign="left"
            border={isActive ? undefined : "1px solid transparent"}
          />
        </HStack>
        <HStack justify="space-between" width="100%">
          <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
            Expiration
          </Text>
          <Text textStyle="text-ui-reg">{formatExpirationDate()}</Text>
        </HStack>
        <HStack justify="space-between" width="100%" align="start">
          <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
            Type
          </Text>
          <VStack align="end">
            <Text textStyle="text-ui-reg">
              {licenseType === "community"
                ? "Community Edition"
                : "Enterprise Edition"}
            </Text>
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              Questions about your license?{" "}
              <TextLink
                as="a"
                href="https://materialize.com/s/chat"
                target="_blank"
                rel="noreferrer"
              >
                Talk to us
              </TextLink>
            </Text>
          </VStack>
        </HStack>
        <HStack justify="space-between" width="100%">
          <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
            Limits
          </Text>
          <Text textStyle="text-ui-reg">{limits}</Text>
        </HStack>
      </VStack>
    </VStack>
  );
};

export const LicenseKeyCTAContent = ({
  selfManagedMode,
  email,
}: {
  selfManagedMode: SelfManagedMode;
  email?: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { track } = useSegment();
  return (
    <VStack
      align="left"
      spacing={3}
      flex={1}
      justifyContent="center"
      textAlign="left"
    >
      <HStack align="center" spacing={3}>
        <BoxIcon color={colors.purple[500]} />
        <Text textStyle="heading-sm">
          {" "}
          {selfManagedMode === "enterprise"
            ? "Get Self Managed"
            : "Need more scale or support?"}
        </Text>
      </HStack>
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        {selfManagedMode === "enterprise"
          ? "The full experience of Materialize, whether you spring it up on your laptop or in your cloud environment."
          : "Enterprise gives you higher limits, advanced features, and expert support."}
      </Text>
      {selfManagedMode === "enterprise" && (
        <GenerateNewLicenseKey licenseType="community" email={email} />
      )}
      {selfManagedMode === "community" && (
        <>
          <Button
            as="a"
            href="https://materialize.com/self-managed"
            target="_blank"
            rel="noreferrer"
            variant="primary"
            alignSelf="flex-start"
            rightIcon={<ArrowRightIcon boxSize={8} />}
            onClick={() => {
              track("Link Click", {
                label: "Talk to us about Enterprise",
                href: "https://materialize.com/self-managed",
              });
            }}
          >
            Talk to us about Enterprise
          </Button>
        </>
      )}
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        Want to learn more?{" "}
        <TextLink
          as="a"
          color={colors.purple[400]}
          href={docUrls["/docs/installation/"]}
          target="_blank"
          rel="noreferrer"
        >
          Visit our documentation
        </TextLink>
      </Text>
    </VStack>
  );
};

export const NoLicenseKeyEmptyState = () => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <LicenseKeyCard loading={false} isError={false}>
      <EmptyListWrapper>
        <EmptyListHeader>
          <Circle p={2} bg={colors.background.secondary}>
            <ActivityIcon color={colors.foreground.secondary} />
          </Circle>
          <EmptyListHeaderContents
            title="No license key found"
            helpText="No license key has been configured for this environment."
          />
        </EmptyListHeader>
      </EmptyListWrapper>
    </LicenseKeyCard>
  );
};
