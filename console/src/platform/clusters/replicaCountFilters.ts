// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Row } from "@tanstack/react-table";

/** The replica column's id, shared by the column, its chip, and the URL. */
export const REPLICA_COLUMN_ID = "replica";

/** The URL parameter carrying the replica count filter. */
export const REPLICA_COUNT_URL_KEY = "replicas";

/**
 * The minimum a visit with no parameter filters by. One replica is the fewest a
 * cluster can have and still run anything, so the list opens on the clusters
 * doing work and leaves reaching for the idle ones to the panel.
 */
export const DEFAULT_MINIMUM_REPLICAS = 1;

/** As much of a row as the replica count filter reads. */
type RowWithReplicas = { cluster: { replicas: unknown[] } };

/**
 * Keeps rows belonging to a cluster with at least `minimum` replicas. A
 * `minimum` of 0 keeps every row, since no cluster has fewer.
 *
 * Counts the row's cluster's replicas, not the row's own replica. The table
 * shows one row per replica, so a row that has a replica always counts as one
 * on its own and a minimum above 1 would match nothing.
 *
 * NOTE: reads the count off the row rather than the column this hangs on, whose
 * accessor returns the replica's name. The value compared is not the value the
 * cell shows.
 */
export const replicaCountFilterFn = <TData extends RowWithReplicas>(
  row: Row<TData>,
  _columnId: string,
  minimum: number,
) => row.original.cluster.replicas.length >= minimum;

/**
 * The minimum a URL asks for, or undefined when it asks for no minimum.
 *
 * An absent or malformed parameter means the default rather than "unfiltered".
 * The parameter is written only when the minimum differs from the default, so a
 * plain visit, which carries no parameter, has to land on it. An explicit `0` is
 * the only way a URL can say "every cluster", which is why clearing the filter
 * writes the parameter instead of dropping it.
 */
export const replicaCountFilterFromUrl = (raw: string | null) => {
  if (raw === null || !/^\d+$/.test(raw)) return DEFAULT_MINIMUM_REPLICAS;
  const minimum = parseInt(raw, 10);
  return minimum > 0 ? minimum : undefined;
};

/**
 * A minimum as its URL parameter value, or undefined when it is the default and
 * so needs no parameter. An undefined `minimum` is no minimum at all, which the
 * URL states as `0`.
 */
export const replicaCountFilterToUrl = (minimum: number | undefined) => {
  const value = minimum ?? 0;
  return value === DEFAULT_MINIMUM_REPLICAS ? undefined : value;
};

/** A minimum stated for display, for example `Replicas ≥ 2`. */
export const replicaCountFilterLabel = (minimum: number) =>
  `Replicas ≥ ${minimum}`;
