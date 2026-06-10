// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import { Setter } from "jotai";
import React from "react";

import { renderComponent } from "~/test/utils";

import { HistorySearch } from "./HistorySearchModal";
import { promptAtom } from "./store/prompt";

function setMockHistory(set: Setter, futureCommands: string[] = []) {
  const commands: string[] = [];
  for (let x = 1; x < 4; x++) {
    commands.push(`SELECT ${x};`);
  }
  set(promptAtom, (prevState) => ({
    ...prevState,
    past: commands.map((c, ix) => ({
      originalValue: c,
      value: c,
      generation: ix,
    })),
    future: futureCommands.map((c, ix) => ({
      originalValue: c,
      value: c,
      generation: ix + commands.length - 1,
    })),
    currentGeneration: commands.length + futureCommands.length,
  }));
}

describe("HistorySearchModal", () => {
  it("Displays a message when there are no history items", async () => {
    await renderComponent(<HistorySearch onSelect={vi.fn()} />);
    const emptyNotice = screen.getByTestId("empty-message");
    expect(emptyNotice).toBeVisible();
    expect(emptyNotice).toHaveTextContent("No queries in your history.");
  });

  it("Displays a message when there are no items that match the filter", async () => {
    await renderComponent(<HistorySearch onSelect={vi.fn()} />);
    const user = userEvent.setup();
    const input = screen.getByTestId("filter-input");
    await user.type(input, "SELECT");
    const emptyNotice = screen.getByTestId("empty-message");
    expect(emptyNotice).toBeVisible();
    expect(emptyNotice).toHaveTextContent("No matching queries");
  });

  it("Displays a list of commands when they exist in the history", async () => {
    await renderComponent(<HistorySearch onSelect={vi.fn()} />, {
      initializeState: ({ set }) => {
        setMockHistory(set);
      },
    });
    const historyList = screen.getByTestId("history-list");
    expect(historyList).toBeVisible();
    const historyListItems = historyList.querySelectorAll("li");
    expect(historyListItems).toHaveLength(3);
    for (const [ix, historyItem] of historyListItems.entries()) {
      // Items are in descending order of recency
      expect(historyItem).toHaveTextContent(`SELECT ${3 - ix};`);
    }
  });

  it("Displays a list of commands when they exist in the history when a user has scrubbed back", async () => {
    await renderComponent(<HistorySearch onSelect={vi.fn()} />, {
      initializeState: ({ set }) => {
        setMockHistory(set, ["SELECT 4;"]);
      },
    });
    const historyList = screen.getByTestId("history-list");
    expect(historyList).toBeVisible();
    const historyListItems = historyList.querySelectorAll("li");
    expect(historyListItems).toHaveLength(4);
    for (const [ix, historyItem] of historyListItems.entries()) {
      // Items are in descending order of recency
      expect(historyItem).toHaveTextContent(`SELECT ${4 - ix};`);
    }
  });

  it("Typing filters the presented history commands", async () => {
    await renderComponent(<HistorySearch onSelect={vi.fn()} />, {
      initializeState: ({ set }) => {
        setMockHistory(set);
      },
    });
    const user = userEvent.setup();
    const input = screen.getByTestId("filter-input");
    await user.type(input, "2");
    const historyList = screen.getByTestId("history-list");
    expect(historyList).toBeVisible();
    const historyListItems = historyList.querySelectorAll("li");
    expect(historyListItems).toHaveLength(1);
    expect(historyListItems.item(0)).toHaveTextContent(`…2;`);
  });
});
