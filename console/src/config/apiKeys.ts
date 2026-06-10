// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { loadStripe } from "@stripe/stripe-js";

export const getLaunchDarklyKey = (consoleEnv: string) => {
  if (consoleEnv === "production") {
    return "63604cf8f9860a0c1f3c7099";
  }
  if (consoleEnv === "staging" || consoleEnv === "preview") {
    // Staging key
    return "6388e8a24ac9d112339757f3";
  }

  // Fall back to development key
  return "6388e8b9750ee71144183456";
};

export const getSegmentApiKey = ({
  consoleEnv,
  isImpersonating,
}: {
  consoleEnv: string;
  isImpersonating: boolean;
}) => {
  if (isImpersonating) {
    return null;
  }

  if (consoleEnv === "production") {
    // Production key
    return "NCe6YQCHM9g04X9yEBUFtoWOuqZU8J1m";
  }

  // Since segment staging isn't hooked up to anything, use the development key for
  // everything that's not production
  return "dGeQYRjmGVsqDI0KIARrAhTvk1BdJJhk";
};

export const getStripePromise = (consoleEnv: string) => {
  if (consoleEnv === "production") {
    // Production key
    return loadStripe("pk_live_eILilEpJ7DCwmw9I4JHuBLEB001qSrxuw0");
  }

  // Stripe development key
  return loadStripe("pk_test_XUB2NGLtbtzx9HQbTKlP1ZCw00FZqUgrXf");
};
