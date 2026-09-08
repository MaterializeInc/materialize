// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { UTCDate } from "@date-fns/utc";
import { addMonths, startOfMonth } from "date-fns";

import { Organization } from "~/api/cloudGlobalApi";
import { components } from "~/api/schemas/global-api";

export function getIsUpgradedPlan(
  planType: components["schemas"]["PlanType"] | undefined,
): planType is Exclude<components["schemas"]["PlanType"], "evaluation"> {
  return planType !== "evaluation";
}

export function calculateNextOnDemandPaymentDate() {
  const nextPaymentDate = startOfMonth(addMonths(new UTCDate(), 1));
  return nextPaymentDate;
}

export function getIsTrialExpired(organization: Organization): boolean {
  return (
    organization.subscription?.type === "evaluation" && organization.blocked
  );
}

export function getIsUpgrading(organization: Organization): boolean {
  // We assume that Trial/evaluation accounts should never have a payment method, otherwise
  // the account would've already been upgraded to an on-demand account via our sync-server.
  // This case only occurs when the payment method has been added to Stripe,
  // but the account is not yet upgraded to an on-demand account.
  return (
    organization.subscription?.type === "evaluation" &&
    (organization.paymentMethods?.length ?? 0) > 0
  );
}
