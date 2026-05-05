// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { Outlet } from "react-router-dom";

import { useTimePeriodMinutes } from "~/hooks/useTimePeriodSelect";

import { LOOKBACK_OPTIONS } from "./constants";
import { MaintainedObjects } from "./MaintainedObjects";
import { MaintainedObjectListItem, useMaintainedObjectsList } from "./queries";

export interface MaintainedObjectsOutletContext {
  data: MaintainedObjectListItem[];
  isLoading: boolean;
}

export const MaintainedObjectsLayout = () => {
  const [lookbackMinutes, setLookbackMinutes] = useTimePeriodMinutes({
    localStorageKey: "maintained-objects-time-period",
    defaultValue: "15",
    timePeriodOptions: LOOKBACK_OPTIONS,
  });

  const { data, isLoading, lagReady, hydrationReady } =
    useMaintainedObjectsList({ lookbackMinutes });

  return (
    <>
      <MaintainedObjects
        objects={data}
        isLoading={isLoading}
        lagReady={lagReady}
        hydrationReady={hydrationReady}
        lookbackMinutes={lookbackMinutes}
        setLookbackMinutes={setLookbackMinutes}
      />
      <Outlet
        context={{ data, isLoading } satisfies MaintainedObjectsOutletContext}
      />
    </>
  );
};
