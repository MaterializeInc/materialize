// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export type SortOrder = "asc" | "desc";

export type SortFn<T> = (a: T, b: T) => number;

export function handleSortOrder<T>(order: SortOrder, sortFn: SortFn<T>) {
  if (order === "desc") {
    return (a: T, b: T) => sortFn(b, a);
  }
  return (a: T, b: T) => sortFn(a, b);
}
