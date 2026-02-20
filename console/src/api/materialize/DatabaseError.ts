// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ErrorCode, ErrorSqlResult } from "~/api/materialize/types";

/**
 * A custom error object for database errors returned by Materialize
 *
 * This implementation is derived from https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Error#custom_error_types
 */
export class DatabaseError extends Error {
  details: ErrorSqlResult;
  skipQueryRetry: boolean = false;

  constructor(
    details: ErrorSqlResult,
    ...params: Parameters<ErrorConstructor>
  ) {
    super(...params);

    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, DatabaseError);
    }

    this.details = details;
    if (!this.message) {
      this.message = details.error.message;
    }
  }
}

export function isPermissonError(result: ErrorSqlResult) {
  return result.error.code === ErrorCode.INSUFFICIENT_PRIVILEGE;
}

export class PermissionError extends DatabaseError {
  skipQueryRetry: boolean = true;
}

export default DatabaseError;
