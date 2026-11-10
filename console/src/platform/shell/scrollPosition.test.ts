// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { isScrollAtBottom } from "./scrollPosition";

describe("isScrollAtBottom", () => {
  it("is true at the exact bottom", () => {
    expect(
      isScrollAtBottom({
        scrollTop: 3583,
        clientHeight: 437,
        scrollHeight: 4020,
      }),
    ).toBe(true);
  });

  it("is true when rounding leaves scrollTop up to 1px short of the bottom", () => {
    // Chromium values for a 437.25px list over 4019.5px of rows after
    // scrollToItem(last, "end").
    expect(
      isScrollAtBottom({
        scrollTop: 3582,
        clientHeight: 437,
        scrollHeight: 4020,
      }),
    ).toBe(true);
    expect(
      isScrollAtBottom({
        scrollTop: 3582.5,
        clientHeight: 437,
        scrollHeight: 4020,
      }),
    ).toBe(true);
  });

  it("is false when scrolled more than 1px above the bottom", () => {
    expect(
      isScrollAtBottom({
        scrollTop: 3581,
        clientHeight: 437,
        scrollHeight: 4020,
      }),
    ).toBe(false);
  });
});
