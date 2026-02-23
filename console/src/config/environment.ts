// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CloudRegion } from "~/store/cloudRegions";

import { ImpersonationConfig } from "./impersonation";

export const getEnvironmentdConfig = ({
  consoleHost,
  cloudRegionOverride,
  impersonation,
}: {
  consoleHost: string;
  cloudRegionOverride: CloudRegion | undefined;
  impersonation: ImpersonationConfig;
}) => {
  if (impersonation) {
    return {
      environmentdHttpAddress: impersonation.environmentdHttpAddress,
      regionId: impersonation.regionId,
    };
  }

  if (cloudRegionOverride) {
    return {
      environmentdHttpAddress: consoleHost,
      regionId: `${cloudRegionOverride.provider}/${cloudRegionOverride.region}`,
    };
  }

  return null;
};
