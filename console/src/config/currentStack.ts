// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import storageAvailable from "~/utils/storageAvailable";

import { buildConstants } from "./buildConstants";

const CURRENT_STACK_KEY = "mz-current-stack";

// We can't use localhost here because of an issue with playwright Safari:
// https://github.com/microsoft/playwright/issues/17368
// local.dev.materialize.com is a real DNS entry pointing to the loopback address
const getE2eTestStack = (hostname: string) => {
  const isLocalStack =
    hostname === "local.dev.materialize.com" || hostname === "localhost";
  const cloudHost = process.env.CLOUD_HOST || "staging.cloud.materialize.com";

  if (isLocalStack) return "local";
  if (cloudHost === "cloud.materialize.com") return "production";
  return "staging";
};

/**
 * The current stack is a string value that represents which cloud resources the console
 * is currently pointing at. This can be changed at runtime, though only with a full
 * page refresh.
 *
 * Possible values:
 * production - Cloud production
 * staging - Cloud staging
 * $USER - Personal stack
 * local - Local development build of environmentd
 * kind - Local cloud stack running in kind
 */
export const getCurrentStack = ({
  hostname,
  isBrowser,
  defaultStack = buildConstants.defaultStack,
}: {
  hostname: string;
  isBrowser: boolean;
  defaultStack?: string;
}) => {
  if (!isBrowser) return getE2eTestStack(hostname);

  if (buildConstants.forceOverrideStack) {
    return buildConstants.forceOverrideStack;
  }
  if (storageAvailable("localStorage")) {
    const stack = window.localStorage.getItem(CURRENT_STACK_KEY);
    if (stack) {
      return stack;
    }
  }
  if (
    hostname.startsWith("staging.") ||
    hostname.startsWith("oidc-test.") ||
    hostname.startsWith("local.console") ||
    hostname.match(/^.*\.preview/)
  ) {
    // matches staging.console.materialize.com
    // or oid-test.console.materialize.com
    // or local.console.materialize.com
    // or *.preview.console.materialize.com
    return "staging";
  }
  if (hostname.startsWith("loadtest.")) {
    return "loadtest";
  }
  if (hostname.startsWith("local.dev.")) {
    return defaultStack;
  }
  const personalStackMatch = hostname.match(/^\w*\.(staging|dev)/);
  if (personalStackMatch) {
    // personal stack, return $USER.$ENV
    return personalStackMatch[0];
  }
  return defaultStack;
};

export const setCurrentStack = (stackName: string) => {
  if (storageAvailable("localStorage")) {
    window.localStorage.setItem(CURRENT_STACK_KEY, stackName);
  }
};
