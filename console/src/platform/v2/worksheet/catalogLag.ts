// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ALLOWED_OBJECT_TYPES } from "~/api/materialize/cluster/materializationLag";
import { LATEST_READING_INTERVAL_MS } from "~/api/materialize/freshness/lagHistory";
import { useMaterializationLag } from "~/platform/clusters/queries";

const LAG_OBJECT_TYPES = new Set<string>(ALLOWED_OBJECT_TYPES);

/** Whether objects of this type report wallclock lag. Views do not. */
export function reportsLag(objectType: string) {
  return LAG_OBJECT_TYPES.has(objectType);
}

/** Latest lag for each of `objectIds`, refreshed as often as a reading lands. */
export function useCatalogLag(objectIds: string[]) {
  const { data } = useMaterializationLag(
    { objectIds },
    { refetchInterval: LATEST_READING_INTERVAL_MS },
  );
  return data?.lagMap;
}
