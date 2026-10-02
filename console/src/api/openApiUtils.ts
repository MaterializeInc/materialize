// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { OpenApiFetchError } from "./OpenApiFetchError";
import { components } from "./schemas/global-api";

export type ApiError = components["schemas"]["ApiError"];

/**
 * Extracts the structured `ApiError` body from a caught error, when the
 * failing endpoint has been migrated to return one (SAS-172). Endpoints
 * that haven't yet return a plain-text or otherwise-shaped body, so callers
 * must still have a generic fallback for when this returns `null`.
 */
export function getApiError(error: unknown): ApiError | null {
  if (
    error instanceof OpenApiFetchError &&
    typeof error.body === "object" &&
    error.body !== null &&
    "reason" in error.body &&
    "message" in error.body &&
    "requestId" in error.body
  ) {
    return error.body as ApiError;
  }
  return null;
}

// `openapi-fetch` splits a response into `data` (success schema) or `error`
// (error-status schema), never both: `data` is always `undefined` on a
// non-2xx response, `error` is always `undefined` on a 2xx one. Callers that
// only ever passed `data` here could never surface a JSON error body, no
// matter what the backend sent, which is the deeper cause behind SAS-149's
// "Empty response" fallback masking real backend messages. `error` is
// optional so existing call sites that don't pass it keep working (they fall
// back to the generic "Empty response" string exactly as before); pass it
// through for endpoints migrated to a structured error body (SAS-172).
export async function handleOpenApiResponse<T, E = unknown>(
  data: T | undefined,
  response: Response,
  error?: E,
) {
  if (!response.ok) {
    throw new OpenApiFetchError(
      response.status,
      error ?? data ?? "Empty response",
    );
  }
  return {
    ...response,
    data,
  };
}
export async function handleOpenApiResponseWithBody<T, E = unknown>(
  data: T | undefined,
  response: Response,
  error?: E,
) {
  if (!response.ok) {
    throw new OpenApiFetchError(
      response.status,
      error ?? data ?? "Empty response",
    );
  }
  if (!data) {
    throw new OpenApiFetchError(
      response.status,
      error ?? data ?? "Empty response",
    );
  }
  return {
    ...response,
    data,
  };
}
