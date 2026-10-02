// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { describe, expect, it, vi } from "vitest";

import { renderComponent } from "~/test/utils";

import { ThresholdInput } from "./FreshnessPage";

const field = () => screen.getByLabelText("Freshness threshold in seconds");

describe("ThresholdInput", () => {
  it("shows the committed threshold in seconds", async () => {
    await renderComponent(
      <ThresholdInput valueMs={2_300} onChange={vi.fn()} />,
    );
    expect(field()).toHaveValue(2.3);
  });

  it("commits once typing stops", async () => {
    const onChange = vi.fn();
    await renderComponent(
      <ThresholdInput valueMs={2_000} onChange={onChange} />,
    );

    await userEvent.clear(field());
    await userEvent.type(field(), "25");

    await waitFor(() => expect(onChange).toHaveBeenCalledWith(25_000));
  });

  it("does not commit the states passed through on the way", async () => {
    // Clearing the field and typing "25" passes through "" and "2". `Number("")`
    // is 0, which marks every object as exceeding, and 2 is a threshold nobody
    // asked for.
    const onChange = vi.fn();
    await renderComponent(
      <ThresholdInput valueMs={2_000} onChange={onChange} />,
    );

    await userEvent.clear(field());
    await userEvent.type(field(), "25");

    await waitFor(() => expect(onChange).toHaveBeenCalledWith(25_000));
    expect(onChange).toHaveBeenCalledTimes(1);
  });

  it("takes a field left empty as no threshold", async () => {
    const onChange = vi.fn();
    await renderComponent(
      <ThresholdInput valueMs={2_000} onChange={onChange} />,
    );

    await userEvent.clear(field());

    await waitFor(() => expect(onChange).toHaveBeenCalledWith(0));
  });

  it("keeps the field responsive while the commit is pending", async () => {
    await renderComponent(
      <ThresholdInput valueMs={2_000} onChange={vi.fn()} />,
    );

    await userEvent.clear(field());
    await userEvent.type(field(), "7");

    // Shown immediately, whatever the debounce is doing.
    expect(field()).toHaveValue(7);
  });
});
