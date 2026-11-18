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
 * Keeps rows whose cluster is owned by one of `selected`, by role id. An empty
 * selection filters nothing.
 */
export const ownerFilterFn = <TData extends { cluster: { ownerId: string } }>(
  row: Row<TData>,
  _columnId: string,
  selected: string[],
) => !selected?.length || selected.includes(row.original.cluster.ownerId);

/**
 * The role ids a URL asks for, or undefined when it names none.
 *
 * NOTE: ids are kept as given. The roles load after the URL is read, so an id
 * can't be checked against them here. One that owns nothing matches no row,
 * and its chip is the way to remove it.
 */
export const ownerFilterFromUrl = (raw: string[]) => {
  const owners = [...new Set(raw.filter((id) => id !== ""))];
  return owners.length ? owners : undefined;
};

/** One selected role stated for display, for example `Owner: materialize`. */
export const ownerFilterLabel = (owner: string) => `Owner: ${owner}`;
