// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useContext } from "react";

import { LaunchDarklyContext } from "~/components/LaunchDarkly";
import { flexibleDeploymentFlags } from "~/config/flexibleDeploymentFlags";
import { useAppConfig } from "~/config/useAppConfig";

export const useFlags = () => {
  const appConfig = useAppConfig();
  const context = useContext(LaunchDarklyContext);

  if (appConfig.mode === "cloud") {
    if (!context) {
      throw new Error("useFlags must be used within LaunchDarklyProvider");
    }
    return context.client.allFlags();
  }

  return flexibleDeploymentFlags;
};
