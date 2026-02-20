// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { MzDataType } from "~/api/materialize/types";
import { buildColumns } from "~/api/mocks/buildSqlQueryHandler";

export const clusterReplicasColumns = buildColumns([
  "name",
  { name: "memoryBytes", type_oid: MzDataType.uint8 },
  { name: "diskBytes", type_oid: MzDataType.uint8 },
  { name: "heapLimit", type_oid: MzDataType.uint8 },
]);

export const arrangmentMemoryColumns = buildColumns([
  "id",
  { name: "size", type_oid: MzDataType.numeric },
  { name: "memoryPercentage", type_oid: MzDataType.numeric },
]);

export const secretListColumns = buildColumns([
  { name: "id" },
  { name: "name" },
  { name: "createdAt", type_oid: MzDataType.timestamptz },
  { name: "databaseName" },
  { name: "schemaName" },
  { name: "isOwner", type_oid: MzDataType.bool },
]);
