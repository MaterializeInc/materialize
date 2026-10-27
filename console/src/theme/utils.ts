// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * A helper function take takes an explicit font size and calculates a line height that adheres to our 8px spacing grid based on this formula:
 * line-height = ceil((size * 1.3) / 8) * 8
 *
 * E.g. 12px font size would have a line-height of 16px
 */
export const lineHeightFromFontSize = (fontSize: number): string => {
  const lh = Math.ceil((fontSize * 1.3) / 8) * 8;
  return `${lh}px`;
};

/**
 * A helper function that calculates the tracking (letter spacing) for a given font size based on this formula:
 * The formula used is an exponential decay function. As the `fontSize` increases, the `tracking` decreases.
 * tracking = -0.0223 + 0.185 * pow(e, -0.1745 * size)
 */
export const trackingFromFontSize = (fontSize: number): string => {
  const tracking = -0.0223 + 0.185 * Math.pow(Math.E, -0.1745 * fontSize);
  return `${tracking}px`;
};
