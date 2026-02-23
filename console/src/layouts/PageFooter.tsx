// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, StackProps } from "@chakra-ui/layout";
import { Text, useTheme } from "@chakra-ui/react";
import React from "react";

import TextLink from "~/components/TextLink";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import { MaterializeTheme } from "~/theme";

import {
  ConsoleImageTag,
  EnvironmentdImageRefTag,
  HelmChartVersionTag,
} from "./footerTagComponents";

const SystemStatusLink = () => {
  return (
    <AppConfigSwitch
      cloudConfigElement={
        <TextLink href="https://status.materialize.com/" target="_blank">
          System Status
        </TextLink>
      }
    />
  );
};

const PageFooterContainer = ({ children, ...props }: StackProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack
      spacing="4"
      bg={colors.background.primary}
      color={colors.foreground.secondary}
      textAlign="center"
      alignItems="center"
      justifyContent="center"
      flexWrap="wrap"
      py="2"
      fontWeight="400"
      fontSize="sm"
      boxShadow="footer"
      borderTopColor={colors.border.primary}
      borderTopWidth="1px"
      {...props}
    >
      {children}
    </HStack>
  );
};

const PrivacyPolicyLink = () => {
  return (
    <TextLink href="https://materialize.com/privacy-policy" target="_blank">
      Privacy Policy
    </TextLink>
  );
};

const CurrentYearLink = () => {
  const currentYear = new Date().getFullYear();
  return <Text>© {currentYear} Materialize, Inc.</Text>;
};

const TermsAndConditionsLink = () => {
  return (
    <TextLink
      href="https://materialize.com/terms-and-conditions"
      target="_blank"
    >
      Terms &amp; Conditions
    </TextLink>
  );
};

export const AuthenticatedPageFooter = ({ children, ...props }: StackProps) => {
  return (
    <PageFooterContainer {...props}>
      <CurrentYearLink />
      <PrivacyPolicyLink />
      <TermsAndConditionsLink />
      <SystemStatusLink />
      <ConsoleImageTag />
      <EnvironmentdImageRefTag />
      <HelmChartVersionTag />
      {children}
    </PageFooterContainer>
  );
};

export const UnauthenticatedPageFooter = ({
  children,
  ...props
}: StackProps) => {
  return (
    <PageFooterContainer {...props}>
      <CurrentYearLink />
      <PrivacyPolicyLink />
      <TermsAndConditionsLink />
      <SystemStatusLink />
      {children}
    </PageFooterContainer>
  );
};

export default AuthenticatedPageFooter;
