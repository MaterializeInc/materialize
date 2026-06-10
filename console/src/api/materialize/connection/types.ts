// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export type Secret = {
  secretName: string;
  databaseName: string;
  schemaName: string;
};

// This type exists for DDL statements that allow a field to be a secret or text
export type TextSecret = {
  secretTextValue: string;
};
