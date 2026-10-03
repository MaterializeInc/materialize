// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { clamp } from "~/util";

/**
 * Bar segment widths for objects' shares of the heap limit, in order. The bar
 * stops at the heap limit even if the shares add up to more.
 */
export const segmentWidths = (percentages: Array<number | null>) => {
  let remaining = 100;
  return percentages.map((percentage) => {
    const width = clamp(percentage ?? 0, 0, remaining);
    remaining -= width;
    return width;
  });
};
