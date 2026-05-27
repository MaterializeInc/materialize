// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

type HeightCache = Map<number, number>;

export const heightByListItemIndex: HeightCache = new Map<number, number>();

export function getTotalHeight(heightCache: HeightCache) {
  let sum = 0;
  for (const [_, height] of heightCache) {
    sum += height;
  }

  return sum;
}

export function clearListItemHeights() {
  heightByListItemIndex.clear();
}

export default heightByListItemIndex;
