// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { OpenApiFetchError } from "./OpenApiFetchError";

export async function handleOpenApiResponse<T>(
  data: T | undefined,
  response: Response,
) {
  if (!response.ok) {
    throw new OpenApiFetchError(response.status, data ?? "Empty response");
  }
  return {
    ...response,
    data,
  };
}
export async function handleOpenApiResponseWithBody<T>(
  data: T | undefined,
  response: Response,
) {
  if (!response.ok) {
    throw new OpenApiFetchError(response.status, data ?? "Empty response");
  }
  if (!data) {
    throw new OpenApiFetchError(response.status, data ?? "Empty response");
  }
  return {
    ...response,
    data,
  };
}
