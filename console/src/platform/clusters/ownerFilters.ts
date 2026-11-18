// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Row } from "@tanstack/react-table";

/** The owner column's id, shared by the column, its chips, and the URL. */
export const OWNER_COLUMN_ID = "owner";

/**
 * The URL parameter carrying the owner filter, repeated once per selected
 * role.
 */
export const OWNER_URL_KEY = "owner[]";

/**
 * Keeps rows whose cluster is owned by one of `selected`, by role name. An
 * empty selection filters nothing.
 *
 * NOTE: a cluster whose owner is unknown is dropped by any active selection,
 * as `hydrationFilterFn` drops a replica it cannot place in a bucket.
 */
export const ownerFilterFn = <TData>(
  row: Row<TData>,
  columnId: string,
  selected: string[],
) => {
  if (!selected?.length) return true;

  const owner = row.getValue<string | null>(columnId);
  return owner !== null && selected.includes(owner);
};

/**
 * The roles a URL asks for, or undefined when it names none.
 *
 * NOTE: names are kept as given. The roles load after the URL is read, so a
 * name can't be checked against them here. One that owns nothing matches no
 * row, and its chip is the way to remove it.
 */
export const ownerFilterFromUrl = (raw: string[]) => {
  const owners = [...new Set(raw.filter((name) => name !== ""))];
  return owners.length ? owners : undefined;
};

/** One selected role stated for display, for example `Owner: materialize`. */
export const ownerFilterLabel = (owner: string) => `Owner: ${owner}`;
