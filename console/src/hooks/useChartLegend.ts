// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

/**
 * Hook for managing the visibility of legend items in a chart. Copies the same
 * interaction UX as CockroachDB's graph legends.
 *
 * @returns An object containing the active status of the legend, the set of visible legend items, and a function to toggle the visibility of a legend item.
 * Assumes each legend item is represented by a unique string key.
 */
export function useChartLegend({
  allLegendItems,
}: {
  allLegendItems: string[];
}) {
  const [visibleLegendItems, setVisibleLegendItems] = React.useState<
    Set<string>
  >(new Set(allLegendItems));

  const toggleLegendItem = (
    key: string,
    event: React.MouseEvent<HTMLDivElement>,
  ) => {
    const isModifierPressed = event.metaKey || event.ctrlKey;

    setVisibleLegendItems((prev) => {
      let newVisibleLegendItems = new Set(prev);

      if (isModifierPressed) {
        if (newVisibleLegendItems.has(key)) {
          newVisibleLegendItems.delete(key);
        } else {
          newVisibleLegendItems.add(key);
        }
      } else {
        if (
          newVisibleLegendItems.size === 1 &&
          newVisibleLegendItems.has(key)
        ) {
          newVisibleLegendItems = new Set(allLegendItems);
        } else {
          newVisibleLegendItems.clear();
          newVisibleLegendItems.add(key);
        }
      }
      return newVisibleLegendItems;
    });
  };

  return {
    visibleLegendItems,
    toggleLegendItem,
  };
}
