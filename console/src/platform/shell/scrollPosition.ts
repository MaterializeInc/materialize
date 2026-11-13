// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export type ScrollMetrics = Pick<
  HTMLElement,
  "scrollTop" | "clientHeight" | "scrollHeight"
>;

/** Whether a scroll container is scrolled to its bottom, within 1px. */
export function isScrollAtBottom({
  scrollTop,
  clientHeight,
  scrollHeight,
}: ScrollMetrics) {
  // AutoSizer and item heights can be fractional, and the browser rounds them
  // separately, so scrollToItem(last, "end") can stop 1px short of the bottom.
  // An exact check then turns off auto-scroll on our own scroll to the bottom.
  return scrollHeight - scrollTop - clientHeight <= 1;
}
