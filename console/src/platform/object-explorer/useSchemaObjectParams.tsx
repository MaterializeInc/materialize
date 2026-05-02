// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useParams } from "react-router-dom";

import { ObjectExplorerParams } from "./routerHelpers";

export type ObjectDetailsParams = ReturnType<typeof useSchemaObjectParams>;

export function useSchemaObjectParams() {
  const { databaseName, schemaName, objectName, id } =
    useParams<ObjectExplorerParams>();
  if (!databaseName || !schemaName || !objectName || !id) {
    // This route won't match unless these parameters are present
    const paramString = JSON.stringify({
      databaseName,
      schemaName,
      objectName,
      id,
    });
    throw new Error(
      `useSchemaObjectParams expects all params to be defined ${paramString}`,
    );
  }
  // Narrow the type
  return {
    databaseName,
    id,
    objectName,
    schemaName,
  };
}
