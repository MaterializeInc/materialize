// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * A custom error that for errors responses from our openapi-fetch library.
 *
 * This implementation is derived from https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Error#custom_error_types
 */
export class OpenApiFetchError extends Error {
  status: number;
  body: string | object;

  constructor(
    status: number,
    body: string | object,
    ...params: Parameters<ErrorConstructor>
  ) {
    super(...params);

    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, OpenApiFetchError);
    }

    this.status = status;
    this.body = body;
    if (!this.message) {
      this.message = `OpenApiFetchError: ${status} - ${body}`;
    }
  }
}
