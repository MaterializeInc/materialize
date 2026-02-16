// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { render, screen, waitFor } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import React from "react";

import { ExtendedRequest } from "~/api/materialize/types";
import server from "~/api/mocks/server";
import { createProviderWrapper } from "~/test/utils";
import { assert } from "~/util";

import TutorialInsertionWidget from "./TutorialInsertionWidget";

const Wrapper = await createProviderWrapper();

const renderComponent = () => {
  return render(
    <Wrapper>
      <TutorialInsertionWidget />
    </Wrapper>,
  );
};

describe("TutorialInsertionWidget", () => {
  it("displays a valid form for valid inputs", async () => {
    renderComponent();
    const user = userEvent.setup();
    const buyerIdInput = screen.getByTestId("account-id-input");
    const submitBtn = screen.getByTestId("account-id-submit");
    const line = screen
      .getByTestId("flipper-command-line")
      .querySelector(".cm-line");
    assert(line);
    await user.type(buyerIdInput, "5");
    expect(submitBtn).toBeEnabled();
    expect(line.textContent).toEqual("INSERT INTO known_flippers VALUES(5);");
    await user.clear(buyerIdInput);
    await user.type(buyerIdInput, "20017");
    expect(submitBtn).toBeEnabled();
    expect(line.textContent).toEqual(
      "INSERT INTO known_flippers VALUES(20017);",
    );
  });

  it("displays an invalid form for invalid inputs", async () => {
    renderComponent();
    const user = userEvent.setup();
    const buyerIdInput = screen.getByTestId("account-id-input");
    const submitBtn = screen.getByTestId("account-id-submit");
    const line = screen
      .getByTestId("flipper-command-line")
      .querySelector(".cm-line");
    assert(line);
    await user.type(buyerIdInput, "bobby tables");
    expect(submitBtn).toBeDisabled();
    expect(line.textContent).toEqual(
      "INSERT INTO known_flippers VALUES(<num>);",
    );
    await user.clear(buyerIdInput);
    await user.type(buyerIdInput, "5ive");
    expect(submitBtn).toBeDisabled();
    expect(line.textContent).toEqual(
      "INSERT INTO known_flippers VALUES(<num>);",
    );
  });

  it("submits a correct query", async () => {
    renderComponent();
    server.use(
      http.post("*/api/sql", async (info) => {
        const body = await info.request.clone().json();
        if (body == null) {
          return undefined;
        }
        const { queries: requestQueries }: ExtendedRequest = body;
        if (
          requestQueries.length === 1 &&
          requestQueries[0].query === `INSERT INTO "known_flippers" VALUES($1)`
        ) {
          return HttpResponse.json({
            results: [
              {
                ok: "INSERT",
                notices: [],
              },
            ],
          });
        }
      }),
    );
    const user = userEvent.setup();
    const buyerIdInput =
      screen.getByTestId<HTMLInputElement>("account-id-input");
    const submitBtn = screen.getByTestId("account-id-submit");
    await user.type(buyerIdInput, "5");
    expect(submitBtn).toBeEnabled();
    await user.click(submitBtn);
    await waitFor(() => expect(buyerIdInput.value).toEqual(""));
  });
});
