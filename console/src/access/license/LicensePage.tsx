// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import {
  MainContentContainer,
  PageHeader,
  PageHeading,
} from "~/layouts/BaseLayout";

import { EnterpriseLicenseInformation } from "./EnterpriseLicenseInformation";
import { SelfManagedLicenseInformation } from "./SelfManagedLicenseInformation";

const LicensePage = () => {
  return (
    <MainContentContainer>
      <PageHeader>
        <PageHeading>License</PageHeading>
      </PageHeader>
      <AppErrorBoundary>
        <AppConfigSwitch
          cloudConfigElement={({ runtimeConfig }) =>
            !runtimeConfig.isImpersonating ? (
              <EnterpriseLicenseInformation user={runtimeConfig.user} />
            ) : (
              <EnterpriseLicenseInformation />
            )
          }
          selfManagedConfigElement={<SelfManagedLicenseInformation />}
        />
      </AppErrorBoundary>
    </MainContentContainer>
  );
};

export default LicensePage;
