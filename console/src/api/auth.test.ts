// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import { canViewUsage } from "./auth";

const baseArgs = {
  isImpersonating: false,
  currentStack: "production",
  hasInvoiceRead: true,
  isBillingVisible: false,
  subscriptionType: "capacity",
};

describe("canViewUsage", () => {
  it("is visible for impersonation users regardless of stack or permissions", () => {
    expect(
      canViewUsage({
        ...baseArgs,
        isImpersonating: true,
        hasInvoiceRead: false,
        subscriptionType: undefined,
      }),
    ).toBe(true);
  });

  it("is visible on non-production stacks regardless of permissions", () => {
    expect(
      canViewUsage({
        ...baseArgs,
        currentStack: "staging",
        hasInvoiceRead: false,
        subscriptionType: undefined,
      }),
    ).toBe(true);
  });

  it("is visible for a personal dev stack", () => {
    expect(
      canViewUsage({
        ...baseArgs,
        currentStack: "rjimeno.dev",
        hasInvoiceRead: false,
        subscriptionType: undefined,
      }),
    ).toBe(true);
  });

  it("on production, requires invoice-read permission and an allowed plan type", () => {
    expect(canViewUsage(baseArgs)).toBe(true);
    expect(canViewUsage({ ...baseArgs, hasInvoiceRead: false })).toBe(false);
    expect(canViewUsage({ ...baseArgs, subscriptionType: "trial" })).toBe(
      false,
    );
  });

  it("on production, only allows the evaluation plan type when billing is visible", () => {
    expect(
      canViewUsage({
        ...baseArgs,
        subscriptionType: "evaluation",
        isBillingVisible: false,
      }),
    ).toBe(false);
    expect(
      canViewUsage({
        ...baseArgs,
        subscriptionType: "evaluation",
        isBillingVisible: true,
      }),
    ).toBe(true);
  });
});
