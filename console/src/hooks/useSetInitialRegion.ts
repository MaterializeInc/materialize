// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useAtom, useSetAtom } from "jotai";
import React from "react";

import {
  currentRegionIdAtom,
  defaultRegionSelector,
} from "~/store/environments";

export const useSetInitialRegion = () => {
  const setCurrentRegionId = useSetAtom(currentRegionIdAtom);
  const [defaultRegion] = useAtom(defaultRegionSelector);
  React.useEffect(() => {
    if (!defaultRegion) return;
    setCurrentRegionId(defaultRegion);
  }, [defaultRegion, setCurrentRegionId]);
};
