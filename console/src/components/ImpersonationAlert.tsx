// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Alert, Text, useTheme } from "@chakra-ui/react";
import * as React from "react";

import { useCurrentOrganization } from "~/api/auth";
import { CloudAppConfig } from "~/config/AppConfig";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import { MaterializeTheme } from "~/theme";

const ImpersonationAlertContent = ({
  appConfig,
}: {
  appConfig: Readonly<CloudAppConfig>;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { organization } = useCurrentOrganization();

  return (
    <Alert
      backgroundColor={colors.background.info}
      borderTopWidth="1px"
      borderTopColor={colors.border.info}
      borderBottomWidth="1px"
      borderBottomColor={colors.border.info}
      py={2}
      fontSize="sm"
      lineHeight="20px"
      color={colors.foreground.primary}
      status="info"
      justifyContent="center"
      data-testid="account-status-alert"
      flexShrink="0"
    >
      <Text>
        You are currently teleported into{" "}
        <Text as="span" textStyle="text-ui-med">
          {organization?.name}
        </Text>{" "}
        ({appConfig.impersonation?.organizationId})
      </Text>
    </Alert>
  );
};

const ImpersonationAlert = () => {
  return (
    <AppConfigSwitch
      cloudConfigElement={({ appConfig }) =>
        appConfig.isImpersonating ? (
          <ImpersonationAlertContent appConfig={appConfig} />
        ) : null
      }
    />
  );
};

export default ImpersonationAlert;
