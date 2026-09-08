// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Matches a number or null, for metrics that may legitimately have no sample.
 *
 * A replica's utilization percentage is the orchestrator's periodic metrics
 * scrape divided by the replica size's allocation, and either half can be
 * missing in a freshly started environment. Use this instead of loosening the
 * surrounding assertion, which keeps every sibling key strict.
 */
export const NUMBER_OR_NULL = {
  asymmetricMatch: (actual: unknown) =>
    actual === null || typeof actual === "number",
  toString: () => "NumberOrNull",
};
