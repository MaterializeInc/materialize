// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { useOutletContext } from "react-router-dom";

import { MaintainedObjectsOutletContext } from "./MaintainedObjectsLayout";

/** The listed subsources and tables that `sourceId` ingests into. */
export const useSourceExports = (sourceId: string) => {
  const { data } = useOutletContext<MaintainedObjectsOutletContext>();
  return React.useMemo(
    () => data.filter((o) => o.parentSource?.id === sourceId),
    [data, sourceId],
  );
};
