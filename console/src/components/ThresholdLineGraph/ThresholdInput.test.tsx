// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { describe, expect, it, vi } from "vitest";

import { renderComponent } from "~/test/utils";

import { ThresholdInput } from "./ThresholdInput";

const field = () => screen.getByLabelText("Threshold in seconds");

describe("ThresholdInput", () => {
  it("shows the committed threshold in seconds", async () => {
    await renderComponent(
      <ThresholdInput valueMs={2_300} onChange={vi.fn()} />,
    );
    expect(field()).toHaveValue(2.3);
  });

  it("reports a typed value", async () => {
    const onChange = vi.fn();
    await renderComponent(
      <ThresholdInput valueMs={2_000} onChange={onChange} />,
    );

    await userEvent.clear(field());
    await userEvent.type(field(), "25");

    // Holding the value back is `useThresholdControl`'s job, not the field's.
    expect(onChange).toHaveBeenLastCalledWith(25_000);
  });

  it("never reports an empty field", async () => {
    // `Number("")` is 0, and a threshold of 0 marks every object as exceeding.
    // Clearing the field to retype must not pass through that.
    const onChange = vi.fn();
    await renderComponent(
      <ThresholdInput valueMs={2_000} onChange={onChange} />,
    );

    await userEvent.clear(field());

    expect(onChange).not.toHaveBeenCalled();
    expect(field()).toHaveValue(null);
  });

  it("reverts an abandoned edit on blur", async () => {
    await renderComponent(
      <ThresholdInput valueMs={2_000} onChange={vi.fn()} />,
    );

    await userEvent.clear(field());
    await userEvent.tab();

    expect(field()).toHaveValue(2);
  });

  it("keeps the field responsive while the commit is pending", async () => {
    await renderComponent(
      <ThresholdInput valueMs={2_000} onChange={vi.fn()} />,
    );

    await userEvent.clear(field());
    await userEvent.type(field(), "7");

    expect(field()).toHaveValue(7);
  });
});
