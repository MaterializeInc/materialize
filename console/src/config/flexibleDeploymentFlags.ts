// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Given the list of feature flags we want to stub in flexible deployment mode
 * is too big, we default all flags to true and specify the ones we want
 * to disable.
 */
export const disabledFlexibleDeploymentFlags: Record<string, boolean> = {};

export const flexibleDeploymentFlags = new Proxy(
  {},
  {
    get: (_, prop: string) => disabledFlexibleDeploymentFlags[prop] ?? true,
  },
) as Record<string, boolean>;
