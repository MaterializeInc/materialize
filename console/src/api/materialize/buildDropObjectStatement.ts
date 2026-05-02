// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";

import { queryBuilder } from "~/api/materialize/db";

import { buildFullyQualifiedObjectName } from ".";
import { DatabaseObject } from "./types";

export type DeletableObjectType =
  | "CLUSTER REPLICA"
  | "CLUSTER"
  | "CONNECTION"
  | "INDEX"
  | "MATERIALIZED VIEW"
  | "SCHEMA"
  | "SECRET"
  | "SINK"
  | "SOURCE"
  | "TABLE"
  | "VIEW";

export function supportsDropCascade(objectType: DeletableObjectType) {
  return objectType !== "CLUSTER REPLICA";
}

export function buildDropObjectStatement({
  dbObject,
  objectType,
}: {
  dbObject: DatabaseObject;
  objectType: DeletableObjectType;
}) {
  const query = sql`DROP ${sql.raw(objectType)} ${buildFullyQualifiedObjectName(
    dbObject,
  )} ${supportsDropCascade(objectType) ? sql.raw("CASCADE") : sql.raw("")}`;
  return query.compile(queryBuilder).sql;
}
