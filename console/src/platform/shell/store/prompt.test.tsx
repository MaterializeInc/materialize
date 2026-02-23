// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { act, renderHook } from "@testing-library/react";
import { Provider as JotaiProvider } from "jotai";
import { useAtom } from "jotai";
import { useAtomCallback } from "jotai/utils";

import * as prompt from "./prompt";

describe("Prompt", () => {
  it("currentPromptValue selector returns the default value from promptAtom", () => {
    const { result: currentPromptValue } = renderHook(
      () => {
        const [value] = useAtom(prompt.currentPromptValue);
        return value;
      },
      { wrapper: JotaiProvider },
    );

    expect(currentPromptValue.current).toEqual("");
  });

  it("setPromptValue updates the promptAtom with the provided value", () => {
    const { result } = renderHook(
      () => {
        const [currentPromptValue] = useAtom(prompt.currentPromptValue);
        const setPromptValue = useAtomCallback(prompt.setPromptValue);
        return { currentPromptValue, setPromptValue };
      },
      { wrapper: JotaiProvider },
    );

    expect(result.current.currentPromptValue).toEqual("");
    act(() => {
      result.current.setPromptValue("New value");
    });

    expect(result.current.currentPromptValue).toEqual("New value");
  });

  it("saveClearPrompt clears the present value and moves it to the past array", () => {
    const { result } = renderHook(
      () => {
        const promptAtom = useAtom(prompt.promptAtom);
        const saveClearPrompt = useAtomCallback(prompt.saveClearPrompt);
        return { promptAtom, saveClearPrompt };
      },
      { wrapper: JotaiProvider },
    );

    act(() => {
      // initialize state
      result.current.promptAtom[1]({
        currentGeneration: 0,
        past: [],
        present: { value: "SELECT 1;", generation: 0 },
        future: [],
      });

      result.current.saveClearPrompt();
    });

    expect(result.current.promptAtom[0]).toEqual({
      currentGeneration: 1,
      past: [{ value: "SELECT 1;", originalValue: "SELECT 1;", generation: 0 }],
      present: { value: "", generation: 1 },
      future: [],
    });
  });

  it("previousPrompt moves the previous value from past array to present and adds the current present value to the future array", () => {
    const { result } = renderHook(
      () => {
        const promptAtom = useAtom(prompt.promptAtom);
        const previousPrompt = useAtomCallback(prompt.previousPrompt);
        return { promptAtom, previousPrompt };
      },
      { wrapper: JotaiProvider },
    );

    act(() => {
      // initialize state
      result.current.promptAtom[1]({
        currentGeneration: 2,
        past: [
          { value: "SELECT 1;", originalValue: "SELECT 1;", generation: 0 },
          { value: "SELECT 2;", originalValue: "SELECT 2;", generation: 1 },
        ],
        present: { value: "SELE", generation: 2 },
        future: [],
      });

      result.current.previousPrompt();
    });

    expect(result.current.promptAtom[0]).toEqual({
      currentGeneration: 2,
      past: [{ value: "SELECT 1;", originalValue: "SELECT 1;", generation: 0 }],
      present: {
        value: "SELECT 2;",
        originalValue: "SELECT 2;",
        generation: 2,
      },
      future: [{ value: "SELE", generation: 2 }],
    });
  });

  it("nextPrompt moves the next value from future array to present and adds the current present value to the past array", () => {
    const { result } = renderHook(
      () => {
        const promptAtom = useAtom(prompt.promptAtom);
        const nextPrompt = useAtomCallback(prompt.nextPrompt);
        return { promptAtom, nextPrompt };
      },
      { wrapper: JotaiProvider },
    );

    act(() => {
      // initialize state
      result.current.promptAtom[1]({
        currentGeneration: 2,
        past: [
          { value: "SELECT 1;", originalValue: "SELECT 1;", generation: 0 },
        ],
        present: { value: "", originalValue: "", generation: 1 },
        future: [
          { value: "SELECT 2;", originalValue: "SELECT 2;", generation: 2 },
        ],
      });

      result.current.nextPrompt();
    });

    expect(result.current.promptAtom[0]).toEqual({
      currentGeneration: 2,
      past: [
        { value: "SELECT 1;", originalValue: "SELECT 1;", generation: 0 },
        { value: "", originalValue: "", generation: 1 },
      ],
      present: {
        value: "SELECT 2;",
        originalValue: "SELECT 2;",
        generation: 2,
      },
      future: [],
    });
  });

  it("do nothing when previous occurs and past array is empty", () => {
    const { result } = renderHook(
      () => {
        const promptAtom = useAtom(prompt.promptAtom);
        const previousPrompt = useAtomCallback(prompt.previousPrompt);
        return { promptAtom, previousPrompt };
      },
      { wrapper: JotaiProvider },
    );

    act(() => {
      // initialize state
      result.current.promptAtom[1]({
        currentGeneration: 1,
        past: [],
        present: {
          value: "SELECT 1;",
          originalValue: "SELECT 1;",
          generation: 0,
        },
        future: [{ value: "SELECT 2;", generation: 1 }],
      });

      result.current.previousPrompt();
    });

    expect(result.current.promptAtom[0]).toEqual({
      currentGeneration: 1,
      past: [],
      present: {
        value: "SELECT 1;",
        originalValue: "SELECT 1;",
        generation: 0,
      },
      future: [{ value: "SELECT 2;", generation: 1 }],
    });
  });

  it("do nothing when next occurs and future array is empty", () => {
    const { result } = renderHook(
      () => {
        const promptAtom = useAtom(prompt.promptAtom);
        const nextPrompt = useAtomCallback(prompt.nextPrompt);
        return { promptAtom, nextPrompt };
      },
      { wrapper: JotaiProvider },
    );

    act(() => {
      // initialize state
      result.current.promptAtom[1]({
        currentGeneration: 1,
        past: [
          { value: "SELECT 1;", originalValue: "SELECT 1;", generation: 0 },
        ],
        present: {
          value: "SELECT 2;",
          generation: 1,
        },
        future: [],
      });

      result.current.nextPrompt();
    });

    expect(result.current.promptAtom[0]).toEqual({
      currentGeneration: 1,
      past: [{ value: "SELECT 1;", originalValue: "SELECT 1;", generation: 0 }],
      present: {
        value: "SELECT 2;",
        generation: 1,
      },
      future: [],
    });
  });

  it("two consecutive previouses", () => {
    const { result } = renderHook(
      () => {
        const promptAtom = useAtom(prompt.promptAtom);
        const previousPrompt = useAtomCallback(prompt.previousPrompt);
        return { promptAtom, previousPrompt };
      },
      { wrapper: JotaiProvider },
    );

    act(() => {
      // initialize state
      result.current.promptAtom[1]({
        currentGeneration: 2,
        past: [
          { value: "SELECT 1;", originalValue: "SELECT 1;", generation: 0 },
          { value: "SELECT 2;", originalValue: "SELECT 2;", generation: 1 },
        ],
        present: { value: "SELE", generation: 2 },
        future: [],
      });
      // Two consecutive undos
      result.current.previousPrompt();
      result.current.previousPrompt();
    });

    expect(result.current.promptAtom[0]).toEqual({
      currentGeneration: 2,
      past: [],
      present: {
        value: "SELECT 1;",
        originalValue: "SELECT 1;",
        generation: 2,
      },
      future: [
        { value: "SELE", generation: 2 },
        { value: "SELECT 2;", originalValue: "SELECT 2;", generation: 2 },
      ],
    });
  });

  it("next, next, previous, type, previous", () => {
    const { result } = renderHook(
      () => {
        const promptAtom = useAtom(prompt.promptAtom);
        const previousPrompt = useAtomCallback(prompt.previousPrompt);
        const nextPrompt = useAtomCallback(prompt.nextPrompt);
        const setPromptValue = useAtomCallback(prompt.setPromptValue);
        return { promptAtom, previousPrompt, setPromptValue, nextPrompt };
      },
      { wrapper: JotaiProvider },
    );

    // initialize state
    act(() => {
      result.current.promptAtom[1]({
        currentGeneration: 2,
        past: [],
        present: {
          value: "SELECT 1;",
          originalValue: "SELECT 1;",
          generation: 2,
        },
        future: [
          { value: "SELECT 3;", originalValue: "SELECT 3;", generation: 2 },
          { value: "SELECT 2;", originalValue: "SELECT 2;", generation: 2 },
        ],
      });
    });

    act(() => {
      result.current.nextPrompt();
    });

    expect(result.current.promptAtom[0]).toEqual({
      currentGeneration: 2,
      past: [{ value: "SELECT 1;", originalValue: "SELECT 1;", generation: 2 }],
      present: {
        value: "SELECT 2;",
        originalValue: "SELECT 2;",
        generation: 2,
      },
      future: [
        { value: "SELECT 3;", originalValue: "SELECT 3;", generation: 2 },
      ],
    });

    act(() => {
      result.current.nextPrompt();
    });

    expect(result.current.promptAtom[0]).toEqual({
      currentGeneration: 2,
      past: [
        { value: "SELECT 1;", originalValue: "SELECT 1;", generation: 2 },
        { value: "SELECT 2;", originalValue: "SELECT 2;", generation: 2 },
      ],
      present: {
        value: "SELECT 3;",
        originalValue: "SELECT 3;",
        generation: 2,
      },
      future: [],
    });

    act(() => {
      result.current.previousPrompt();
    });

    expect(result.current.promptAtom[0]).toEqual({
      currentGeneration: 2,
      past: [{ value: "SELECT 1;", originalValue: "SELECT 1;", generation: 2 }],
      present: {
        value: "SELECT 2;",
        originalValue: "SELECT 2;",
        generation: 2,
      },
      future: [
        { value: "SELECT 3;", originalValue: "SELECT 3;", generation: 2 },
      ],
    });

    act(() => {
      result.current.setPromptValue("SELECT 4;");
    });

    expect(result.current.promptAtom[0]).toEqual({
      currentGeneration: 2,
      past: [{ value: "SELECT 1;", originalValue: "SELECT 1;", generation: 2 }],
      present: {
        value: "SELECT 4;",
        originalValue: "SELECT 2;",
        generation: 2,
      },
      future: [
        { value: "SELECT 3;", originalValue: "SELECT 3;", generation: 2 },
      ],
    });

    act(() => {
      result.current.previousPrompt();
    });

    expect(result.current.promptAtom[0]).toEqual({
      currentGeneration: 2,
      past: [],
      present: {
        value: "SELECT 1;",
        originalValue: "SELECT 1;",
        generation: 2,
      },
      future: [
        { value: "SELECT 3;", originalValue: "SELECT 3;", generation: 2 },
        { value: "SELECT 4;", originalValue: "SELECT 2;", generation: 2 },
      ],
    });
  });

  it("saveClearPrompt on a previous record appends a copy to the end of the past array", () => {
    const { result } = renderHook(
      () => {
        const promptAtom = useAtom(prompt.promptAtom);
        const previousPrompt = useAtomCallback(prompt.previousPrompt);
        const nextPrompt = useAtomCallback(prompt.nextPrompt);
        const setPromptValue = useAtomCallback(prompt.setPromptValue);
        const saveClearPrompt = useAtomCallback(prompt.saveClearPrompt);
        return {
          promptAtom,
          nextPrompt,
          previousPrompt,
          saveClearPrompt,
          setPromptValue,
        };
      },
      { wrapper: JotaiProvider },
    );

    act(() => {
      // initialize state
      result.current.promptAtom[1]({
        currentGeneration: 1,
        past: [
          { value: "SELECT 1;", originalValue: "SELECT 1;", generation: 0 },
        ],
        present: { value: "", generation: 1 },
        future: [],
      });
    });

    act(() => {
      result.current.previousPrompt();
    });

    expect(result.current.promptAtom[0]).toEqual({
      currentGeneration: 1,
      past: [],
      present: {
        value: "SELECT 1;",
        originalValue: "SELECT 1;",
        generation: 1,
      },
      future: [{ value: "", generation: 1 }],
    });

    act(() => {
      result.current.setPromptValue("SELECT 2;");
    });

    expect(result.current.promptAtom[0]).toEqual({
      currentGeneration: 1,
      past: [],
      present: {
        value: "SELECT 2;",
        originalValue: "SELECT 1;",
        generation: 1,
      },
      future: [{ value: "", generation: 1 }],
    });

    act(() => {
      result.current.saveClearPrompt();
    });

    expect(result.current.promptAtom[0]).toEqual({
      currentGeneration: 2,
      past: [
        { value: "SELECT 1;", originalValue: "SELECT 1;", generation: 1 },
        { value: "SELECT 2;", originalValue: "SELECT 2;", generation: 1 },
      ],
      present: { value: "", generation: 2 },
      future: [],
    });
  });
});
