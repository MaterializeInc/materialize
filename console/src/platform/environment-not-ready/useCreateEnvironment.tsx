// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useToast } from "@chakra-ui/react";
import * as Sentry from "@sentry/react";
import { useAtom, useSetAtom } from "jotai";
import retry from "p-retry";
import React from "react";

import { createRegion } from "~/api/cloudRegionApi";
import { cloudRegionsSelector } from "~/store/cloudRegions";
import {
  currentRegionIdAtom,
  maybeEnvironmentForRegion,
} from "~/store/environments";
import { assert } from "~/util";

export type CreateRegion = (regionId: string) => Promise<void>;

// Relies on the environment health polling in usePollEnvironmentHealth
const useCreateEnvironment = () => {
  const [creatingRegionId, setCreatingRegionId] = React.useState<string>();

  const toast = useToast({ position: "top" });
  const [cloudRegions] = useAtom(cloudRegionsSelector);
  const setCurrentRegion = useSetAtom(currentRegionIdAtom);

  const createRegionCallback = React.useCallback(
    async (regionId: string) => {
      const region = cloudRegions.get(regionId);
      assert(region);
      setCreatingRegionId(regionId);
      setCurrentRegion(regionId);
      try {
        await retry(
          async () => {
            await createRegion(region.regionApiUrl);
          },
          // 5 tries total, no backoff
          { retries: 4, minTimeout: 0, factor: 1 },
        );
      } catch (error) {
        Sentry.captureException(error);
        console.error(error);
        setCreatingRegionId(undefined);
        toast({
          title: "Failed to enable region.",
          status: "error",
        });
      }
    },
    [cloudRegions, setCurrentRegion, toast],
  );

  const [newEnvironment] = useAtom(
    maybeEnvironmentForRegion({
      regionId: creatingRegionId,
    }),
  );

  React.useEffect(() => {
    if (
      creatingRegionId &&
      newEnvironment &&
      newEnvironment.state !== "disabled"
    ) {
      setCreatingRegionId(undefined);
    }
  }, [creatingRegionId, newEnvironment]);

  return {
    creatingRegionId,
    createRegion: createRegionCallback,
  };
};

export default useCreateEnvironment;
