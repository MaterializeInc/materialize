// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Object types we treat as "maintained" — sources, materialized views,
 * indexes, and sinks. Values match `mz_objects.type` (and the hyphenated
 * form used by `mz_object_fully_qualified_names.object_type`).
 */
export const MAINTAINED_OBJECT_TYPES = [
  "source",
  "materialized-view",
  "index",
  "sink",
] as const;

export type MaintainedObjectType = (typeof MAINTAINED_OBJECT_TYPES)[number];

/**
 * Object types the maintained-objects list shows. Adds the collections a
 * source ingests into: subsources, and tables created with `CREATE TABLE ...
 * FROM SOURCE`. Each has its own frontier, so it can lag behind its source.
 */
export const MAINTAINED_OBJECT_LIST_TYPES = [
  "source",
  "subsource",
  "table",
  "materialized-view",
  "index",
  "sink",
] as const;

export type MaintainedObjectListType =
  (typeof MAINTAINED_OBJECT_LIST_TYPES)[number];
