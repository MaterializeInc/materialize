// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { render, screen } from "@testing-library/react";
import React from "react";

import { createProviderWrapper } from "~/test/utils";

import { shellStateAtom } from "./store/shell";
import Tutorial from "./Tutorial";

const renderTutorial = async (activeTutorial: "quickstart" | "academy") => {
  const Wrapper = await createProviderWrapper({
    initializeState: async (store) => {
      store.set(shellStateAtom, (prev) => ({
        ...prev,
        activeTutorial,
        currentTutorialStep: 0,
        tutorialVisible: true,
      }));
    },
  });
  return render(
    <Wrapper>
      <Tutorial runCommand={() => undefined} />
    </Wrapper>,
  );
};

describe("Tutorial", () => {
  it("renders Quickstart content when activeTutorial=quickstart", async () => {
    await renderTutorial("quickstart");
    // The Quickstart intro page mentions "auction data" prominently and the
    // section label is "QUICKSTART".
    expect(await screen.findByText("QUICKSTART")).toBeInTheDocument();
  });

  it("renders MZ Academy content when activeTutorial=academy", async () => {
    await renderTutorial("academy");
    expect(await screen.findByText("MZ ACADEMY")).toBeInTheDocument();
    // The first academy step's heading is "Welcome to MZ Academy".
    expect(
      await screen.findByText("Welcome to MZ Academy"),
    ).toBeInTheDocument();
  });
});
