// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { NULL_DATABASE_NAME } from "./constants";
import { SupportedObjectType } from "./ObjectExplorerNode";

export const OBJECT_TYPE_TO_SLUG = {
  view: "views",
  index: "indexes",
  table: "tables",
  secret: "secrets",
  source: "sources",
  sink: "sinks",
  connection: "connections",
  "materialized-view": "materialized-views",
} as const;

export function relativeDatabasePath(params: { databaseName: string | null }) {
  return `${encodeURIComponent(params.databaseName ?? NULL_DATABASE_NAME)}`;
}

export function relativeSchemaPath(params: {
  databaseName: string | null;
  schemaName: string;
}) {
  return `${relativeDatabasePath(params)}/schemas/${encodeURIComponent(
    params.schemaName,
  )}`;
}

export function relativeObjectTypePath(params: {
  databaseName: string | null;
  schemaName: string;
  objectType: string;
}) {
  const objectLabel =
    OBJECT_TYPE_TO_SLUG[params.objectType as SupportedObjectType] ?? "unknown";
  return `${relativeSchemaPath(params)}/${objectLabel}`;
}

export function relativeObjectPath(params: {
  databaseName: string | null;
  schemaName: string;
  objectType: string;
  objectName: string;
  id: string;
}) {
  return `${relativeObjectTypePath(params)}/${encodeURIComponent(
    params.objectName,
  )}/${params.id}`;
}

export type ObjectExplorerParams = {
  databaseName?: string;
  schemaName?: string;
  objectName?: string;
  objectType?: string;
  id?: string;
};
