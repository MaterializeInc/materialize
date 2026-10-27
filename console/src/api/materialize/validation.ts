// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Matches an http or https string with subdomains and port numbers.
 * An example: https://rp-f00000bar.data.vectorized.cloud:30993/
 */
export const HTTP_URL_WITH_EXPLICIT_ROOT_PATH_REGEX =
  /^https?:\/\/[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)*(:\d+)?(\/*)?$/;

/**
 * Matches a valid HTTP header field name.
 */
export const HTTP_HEADER_FIELD_NAME_REGEX = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
