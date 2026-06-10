// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Button,
  Code,
  HStack,
  Text,
  TextProps,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useAtomValue } from "jotai";
import React, { PropsWithChildren } from "react";
import { Link } from "react-router-dom";

import {
  hasEnvironmentWritePermission,
  useCurrentOrganization,
} from "~/api/auth";
import MaterializeErrorCode from "~/api/materialize/errorCodes";
import NetworkPolicyError from "~/api/materialize/NetworkPolicyError";
import SupportLink from "~/components/SupportLink";
import TextLink from "~/components/TextLink";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import { User } from "~/external-library-wrappers/frontegg";
import { useFlags } from "~/hooks/useFlags";
import docUrls from "~/mz-doc-urls.json";
import { currentEnvironmentState } from "~/store/environments";
import { MaterializeTheme } from "~/theme";

const BlockedContent = ({
  title,
  children,
}: PropsWithChildren<{ title: string }>) => {
  return (
    <VStack flex={1} h="full" w="full" data-testid="blocked-status-alert">
      <VStack
        spacing={6}
        m={6}
        h="full"
        w="full"
        alignItems="center"
        justifyContent="center"
      >
        <VStack spacing={4} align="flex-start" maxWidth="500px">
          <Text as="h1" textStyle="heading-lg">
            {title}
          </Text>
          {children}
        </VStack>
      </VStack>
    </VStack>
  );
};

const BlockedText = (props: TextProps) => {
  return <Text as="h4" textStyle="text-base" fontSize="16px" {...props} />;
};

type NetworkBlockedDetails = {
  ip: string | null;
};

const IP_REGEX =
  /\b(?:\d{1,3}\.){3}\d{1,3}\b|\b(?:[A-Fa-f0-9]{1,4}:){7}[A-Fa-f0-9]{1,4}\b/g;

function parseNetworkBlock(error: NetworkPolicyError): NetworkBlockedDetails {
  const matches = error.detail?.match(IP_REGEX);
  if (matches) {
    return { ip: matches[0] ?? null };
  }
  return { ip: null };
}

function useNetworkPolicyBlock(): NetworkBlockedDetails | null {
  const environment = useAtomValue(currentEnvironmentState);
  if (environment && environment.state === "enabled") {
    for (const error of environment.status.errors) {
      if (
        error.details instanceof NetworkPolicyError &&
        error.details.code ===
          MaterializeErrorCode.NETWORK_POLICY_SESSION_DENIED
      ) {
        return parseNetworkBlock(error.details);
      }
    }
  }
  return null;
}

const BlockedStateContent = ({ user }: { user: User }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { organization } = useCurrentOrganization();
  const isTrialCustomer = organization?.subscription?.type === "evaluation";
  const networkBlock = useNetworkPolicyBlock();
  const isOrgAdmin = hasEnvironmentWritePermission(user);
  const flags = useFlags();

  const isBillingEnabled = flags["billing-ui-3756"];

  if (networkBlock !== null) {
    return (
      <BlockedContent title="Connection blocked">
        <BlockedText color={colors.foreground.secondary}>
          Your IP address{" "}
          {networkBlock.ip ? (
            <>
              (<Code>{networkBlock.ip}</Code>){" "}
            </>
          ) : null}
          is not included in the list of allowed IPs for this region&apos;s
          Network Policy.
        </BlockedText>
        <BlockedText>
          Learn more about Network Policies in our{" "}
          <TextLink href={docUrls["/docs/"]} isExternal>
            docs
          </TextLink>
          .
        </BlockedText>
        <BlockedText>
          If you believe this is an error, or you&apos;re accidentally locked
          out, please contact{" "}
          {isOrgAdmin ? (
            <SupportLink data-testid="admin-support-link">support</SupportLink>
          ) : (
            "your administrators"
          )}
          .
        </BlockedText>
      </BlockedContent>
    );
  }
  return (
    <BlockedContent
      title={
        isTrialCustomer
          ? "Your trial has ended"
          : "Your Materialize plan has lapsed or is upgrading"
      }
    >
      <BlockedText color={colors.foreground.secondary}>
        {isTrialCustomer
          ? "But, we can help get you back to operating in real-time in no time!"
          : "Your organization's access is currently restricted or is upgrading. If restricted, please contact us to reactivate your account and restore access to your environment."}
      </BlockedText>

      {isBillingEnabled && isTrialCustomer ? (
        <HStack>
          <SupportLink>
            <Button size="sm" variant="secondary" alignSelf="flex-end">
              Contact support
            </Button>
          </SupportLink>
          {isOrgAdmin && (
            <Button as={Link} to="/usage/billing" variant="primary" size="sm">
              Upgrade plan
            </Button>
          )}
        </HStack>
      ) : (
        <SupportLink>
          <Button size="lg" variant="primary" alignSelf="flex-end">
            Talk with our team
          </Button>
        </SupportLink>
      )}
    </BlockedContent>
  );
};

const BlockedState = () => {
  return (
    <AppConfigSwitch
      cloudConfigElement={({ runtimeConfig }) =>
        runtimeConfig.isImpersonating ? null : (
          <BlockedStateContent user={runtimeConfig.user} />
        )
      }
    />
  );
};

export default BlockedState;
