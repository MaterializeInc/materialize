// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import { lirGroupColor, prettyPrintChannelType } from "./nodeStyle";

describe("lirGroupColor", () => {
  it("is deterministic for the same lirId", () => {
    expect(lirGroupColor("42")).toEqual(lirGroupColor("42"));
  });

  it("differs for different lirIds often enough to be useful", () => {
    const colors = new Set(["1", "2", "3", "4", "5", "6"].map(lirGroupColor));
    expect(colors.size).toBeGreaterThan(1);
  });
});

describe("prettyPrintChannelType", () => {
  it("strips module paths, aliases Diff/Error, and brackets Vec", () => {
    expect(
      prettyPrintChannelType(
        "alloc::vec::Vec<(mz_repr::row::Row, mz_repr::timestamp::Timestamp, mz_ore::overflowing::Overflowing<i64>)>",
      ),
    ).toEqual("[(Row, Timestamp, Diff)]");
  });

  it("handles the error-channel variant", () => {
    expect(
      prettyPrintChannelType(
        "alloc::vec::Vec<(mz_compute::render::errors::DataflowErrorSer, mz_repr::timestamp::Timestamp, mz_ore::overflowing::Overflowing<i64>)>",
      ),
    ).toEqual("[(Error, Timestamp, Diff)]");
  });

  it("unwraps nested Vec and unrecognized generics without mangling them", () => {
    expect(
      prettyPrintChannelType(
        "alloc::vec::Vec<alloc::rc::Rc<differential_dataflow::trace::implementations::ord_neu::val_batch::OrdValBatch<mz_row_spine::spines::RowRowLayout<((mz_repr::row::Row, mz_repr::row::Row), mz_repr::timestamp::Timestamp, mz_ore::overflowing::Overflowing<i64>)>>>>",
      ),
    ).toEqual("[Rc<OrdValBatch<RowRowLayout<((Row, Row), Timestamp, Diff)>>>]");
  });
});
