// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// The dataflow the browser tests visualize. `mzcompose.py` creates the index
// and waits for its dataflow to reach introspection before Playwright starts,
// and passes these names in the environment so both ends agree on one
// definition. See the comments there for why the tests pin a dataflow instead
// of expanding whichever row the page happens to list first.
//
// The fallbacks keep `npx playwright test` usable against a Materialize started
// by hand, as long as the same fixture exists there.
//
// The fixture lives on `quickstart`, the only user cluster in the composition,
// which is also the one an unpinned page has to select. That is not a
// coincidence: the visualizer exists to show you your own dataflows.

export const FIXTURE_CLUSTER = process.env.FIXTURE_CLUSTER ?? 'quickstart';
export const FIXTURE_REPLICA = process.env.FIXTURE_REPLICA ?? 'r1';
export const FIXTURE_VIEW =
  process.env.FIXTURE_VIEW ?? 'visualizer_fixture_view';
export const FIXTURE_INDEX =
  process.env.FIXTURE_INDEX ?? 'visualizer_fixture_idx';

// How the fixture's dataflow is named in the visualizer's dataflow table.
export const FIXTURE_DATAFLOW = `Dataflow: materialize.public.${FIXTURE_INDEX}`;

/** A page URL that pins the cluster replica the page should show. */
export function replicaPage(
  path: string,
  cluster: string,
  replica: string
): string {
  const params = new URLSearchParams({
    cluster_name: cluster,
    replica_name: replica,
  });
  return `${path}?${params}`;
}

/**
 * A page URL pinned to the fixture's replica, so a test never depends on which
 * replica the page picks by default.
 */
export function fixturePage(path: string): string {
  return replicaPage(path, FIXTURE_CLUSTER, FIXTURE_REPLICA);
}
