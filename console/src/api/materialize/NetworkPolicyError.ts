// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { PostgresError } from "pg-error-enum";

import MaterializeErrorCode from "./errorCodes";

class NetworkPolicyError extends Error {
  code: MaterializeErrorCode | PostgresError;
  detail: string | undefined;

  constructor(
    code: MaterializeErrorCode | PostgresError,
    detail: string | undefined,
    ...params: Parameters<ErrorConstructor>
  ) {
    super(...params);
    this.code = code;
    this.detail = detail;
  }
}

export default NetworkPolicyError;
