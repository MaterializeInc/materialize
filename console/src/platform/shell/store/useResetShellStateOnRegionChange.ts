// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { usePrevious } from "@chakra-ui/react";
import { useAtom } from "jotai";
import { useAtomCallback } from "jotai/utils";
import { useEffect } from "react";

import { currentRegionIdAtom } from "~/store/environments";

import { resetShellState } from "./shell";

export const useResetShellStateOnRegionChange = () => {
  const resetShellStateCallback = useAtomCallback(resetShellState);
  const [currentRegionId] = useAtom(currentRegionIdAtom);

  const prevCurrentRegionId = usePrevious(currentRegionId);

  useEffect(() => {
    if (prevCurrentRegionId !== currentRegionId) {
      resetShellStateCallback();
    }
  }, [currentRegionId, resetShellStateCallback, prevCurrentRegionId]);
};
