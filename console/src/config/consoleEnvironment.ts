// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Returns a string that represents the environment of the console build, which is
 * separate from the current stack. For example, you might be on staging console, and
 * use the stack switcher to point at a personal stack.
 */
export const getConsoleEnvironment = ({
  hostname,
  isImpersonating,
}: {
  hostname: string;
  isImpersonating: boolean;
}) => {
  if (isImpersonating) {
    return "production";
  }
  if (hostname === "console.materialize.com") {
    return "production";
  }
  if (hostname === "staging.console.materialize.com") {
    return "staging";
  }
  if (hostname.match(/^.*\.preview/)) {
    // *.preview.console.materialize.com
    return "preview";
  }
  return "development";
};
