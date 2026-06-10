// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { useSelfManagedSubscription } from "~/api/auth";
import { OpenApiFetchError } from "~/api/OpenApiFetchError";
import {
  LicenseInformationContent,
  LicenseKeyCard,
  LicenseKeyCTAContent,
} from "~/components/licenseComponents";
import { User } from "~/external-library-wrappers/frontegg";

export const EnterpriseLicenseInformation = ({ user }: { user?: User }) => {
  const { subscription, isLoading, isError, error } =
    useSelfManagedSubscription();

  // 404 means no subscription found - this is not an error state, show CTA
  const is404Error = error instanceof OpenApiFetchError && error.status === 404;
  const shouldShowError = isError && !is404Error;

  return (
    <LicenseKeyCard loading={isLoading} isError={shouldShowError}>
      {subscription ? (
        <LicenseInformationContent
          canIssueLicense={Boolean(
            subscription.endDate && new Date() < new Date(subscription.endDate),
          )}
          expirationDate={subscription.endDate}
          limits="Unlimited"
          licenseType="enterprise"
        />
      ) : (
        <LicenseKeyCTAContent
          selfManagedMode="enterprise"
          email={user?.email}
        />
      )}
    </LicenseKeyCard>
  );
};
