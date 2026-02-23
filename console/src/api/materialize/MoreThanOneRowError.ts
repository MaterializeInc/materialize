// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export interface MoreThanOneRowErrorDetails {
  rows: number;
  skipQueryRetry?: boolean;
}
/**
 * A custom error object for queries that are expected to return a specific number of rows.
 *
 * This implementation is derived from https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Error#custom_error_types
 */
export class MoreThanOneRowError extends Error {
  details: MoreThanOneRowErrorDetails;
  skipQueryRetry: boolean;

  constructor(
    details: MoreThanOneRowErrorDetails,
    ...params: Parameters<ErrorConstructor>
  ) {
    super(...params);

    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, MoreThanOneRowError);
    }

    this.details = details;
    this.skipQueryRetry = details.skipQueryRetry ?? false;
    if (!this.message) {
      this.message = "Query expected exactly one row, got " + details.rows;
    }
  }
}

export function assertNoMoreThanOneRow(
  rowCount: number,
  options?: { skipQueryRetry: boolean },
) {
  if (rowCount > 1) {
    throw new MoreThanOneRowError({
      rows: rowCount,
      skipQueryRetry: options?.skipQueryRetry,
    });
  }
}
