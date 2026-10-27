// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export interface NoRowsErrorDetails {
  rows: number;
  skipQueryRetry?: boolean;
}
/**
 * A custom error object for queries that are expected to return at least one row.
 *
 * This implementation is derived from https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Error#custom_error_types
 */
export class NoRowsError extends Error {
  details: NoRowsErrorDetails;
  skipQueryRetry: boolean;

  constructor(
    details: NoRowsErrorDetails,
    ...params: Parameters<ErrorConstructor>
  ) {
    super(...params);

    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, NoRowsError);
    }

    this.details = details;
    this.skipQueryRetry = details.skipQueryRetry ?? false;
    if (!this.message) {
      this.message = "Query expected at least one row, got " + details.rows;
    }
  }
}

export function assertAtLeastOneRow(
  rowCount: number,
  options?: { skipQueryRetry: boolean },
) {
  if (rowCount !== 1) {
    throw new NoRowsError({
      rows: rowCount,
      skipQueryRetry: options?.skipQueryRetry,
    });
  }
}
