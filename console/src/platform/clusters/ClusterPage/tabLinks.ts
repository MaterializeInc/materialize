// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { To } from "react-router-dom";

import { REPLICA_SEARCH_PARAM } from "./useSelectedReplica";

export const WORKERS_TAB_PATH = "workers";

/** The search string that selects `replicaName`, or none for all replicas. */
export const replicaSearch = (replicaName: string | undefined) =>
  replicaName === undefined
    ? ""
    : `?${new URLSearchParams({ [REPLICA_SEARCH_PARAM]: replicaName })}`;

/** The Workers tab on `replicaName`, linked from the Metrics tab. */
export const workersTabLink = (replicaName: string | undefined): To => ({
  pathname: WORKERS_TAB_PATH,
  search: replicaSearch(replicaName),
});

/** A link's target and the router state it arrives with. */
export interface TabLink {
  to: To;
  state?: { openProcesses: true };
}

/**
 * The Metrics tab on `replicaName` with its Processes drawer open, linked from
 * the Workers tab.
 */
export const processesTabLink = (replicaName: string): TabLink => ({
  to: { pathname: "..", search: replicaSearch(replicaName) },
  state: { openProcesses: true },
});

/** Whether a visit to the Metrics tab came from `processesTabLink`. */
export const opensProcesses = (state: unknown) =>
  typeof state === "object" &&
  state !== null &&
  "openProcesses" in state &&
  state.openProcesses === true;
