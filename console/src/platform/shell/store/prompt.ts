// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";
import { atom, Getter, Setter } from "jotai";

export type PromptSetHandler = (newPrompt: string) => void;

/*
 * We use the concept of a 'generation' to refer to a current prompt editing session. When a query
 * is submitted, the generation advances. This allows us to provide our users with a readline-esque
 * set of editing behaviors, where they can scrub through their Shell history and edit the buffers
 * for each command. The next time a query is submitted, those staged-but-not-submitted commands
 * are considered stale because they're of the previous generation and don't represent the
 * original query which created the history item.
 */

export type PromptValue = {
  /** The editor-bound, displayed value */
  value: string;
  /** The value this prompt item had when it was first committed to the scrollback history */
  originalValue?: string;
  /** The last prompt generation the value was stamped in */
  generation: number;
};

type Prompt = {
  // An incrementing identifier for the prompt history
  currentGeneration: number;
  past: PromptValue[];
  present: PromptValue;
  future: PromptValue[];
};

export const defaultState: Prompt = {
  currentGeneration: 0,
  past: [],
  present: {
    value: "",
    generation: 0,
  },
  future: [],
};

export const promptAtom = atom<Prompt>(defaultState);

export const currentPromptValue = atom((get) => get(promptAtom).present.value);

export const setPromptValue = (
  _get: Getter,
  set: Setter,
  value: string,
  onSet?: PromptSetHandler,
) => {
  set(promptAtom, (prevState) => {
    onSet && onSet(value);
    return {
      ...prevState,
      present: {
        ...prevState.present,
        value,
      },
    };
  });
};

/**
 * Get the target prompt (i.e., next or previous) from a past or future collection of prompts, managing
 * unstaged state.
 *
 * This is effectively the last element from the supplied array.
 */
function getHistoricalTargetPrompt(
  collection: PromptValue[],
  currentGeneration: number,
): PromptValue | null {
  let targetPrompt = collection.at(collection.length - 1);
  if (!targetPrompt) return null;
  if (
    targetPrompt.originalValue &&
    targetPrompt.value !== targetPrompt.originalValue
  ) {
    // this prompt has been edited
    if (targetPrompt.generation !== currentGeneration) {
      // ...and we've advanced generations, so the unstaged command is stale. Reset the value.
      targetPrompt = {
        ...targetPrompt,
        generation: currentGeneration,
        value: targetPrompt.originalValue,
      };
    }
  } else if (
    targetPrompt.originalValue &&
    targetPrompt.value === targetPrompt.originalValue
  ) {
    // This prompt has not been edited, but update the generation so future edits within the current
    // generation aren't accidentally erased by scrubbing through
    targetPrompt = {
      ...targetPrompt,
      generation: currentGeneration,
    };
  }
  return targetPrompt;
}

export const saveClearPrompt = (
  _get: Getter,
  set: Setter,
  onSet?: PromptSetHandler,
) => {
  set(promptAtom, (prevState) => {
    const curPrompt = prevState.present;
    const pastPrompts = [...prevState.past];
    // When a user is submitting a new prompt, we need to append it to the _end_ of all tracked history
    //  In the event they're editing or resubmitting a historical prompt, we need to place it _after_
    //  the future states to ensure chronological history is maintained.
    if (prevState.future.length > 0) {
      // we're running a command that is either historical or forked from one, first restore the original
      if (curPrompt.originalValue) {
        pastPrompts.push({
          ...curPrompt,
          value: curPrompt.originalValue,
        });
      } else {
        console.error(
          `Original prompt does not have a value: '${curPrompt.originalValue}'`,
        );
        Sentry.captureException(
          new Error("Edited shell historical prompt value is empty"),
          {
            extra: {
              originalValueLength: curPrompt.originalValue?.length ?? -1,
              newValueLength: curPrompt.value.length,
              generation: curPrompt.generation,
              numFuture: prevState.future.length,
            },
          },
        );
      }
      // Then apppend the future items in chronological order. Drop the most recent one (by never reaching
      // index 0) since it represents an unstaged command that wasn't run.
      for (let ix = prevState.future.length - 1; ix > 0; ix--) {
        pastPrompts.push(prevState.future[ix]);
      }
    }
    // Finally, append the current prompt
    pastPrompts.push({
      value: curPrompt.value,
      originalValue: curPrompt.value,
      generation: prevState.currentGeneration,
    });

    onSet && onSet("");
    return {
      currentGeneration: prevState.currentGeneration + 1,
      past: pastPrompts,
      present: {
        value: "",
        generation: prevState.currentGeneration + 1,
      },
      future: [],
    };
  });
};

export const previousPrompt = (
  get: Getter,
  set: Setter,
  onSet?: PromptSetHandler,
) => {
  // const snapshotState = get(promptAtom);

  if (
    get(promptAtom).past.length === 0
    // snapshotState.state !== "hasValue" ||
    // snapshotState.contents.past.length === 0
  ) {
    return;
  }

  set(promptAtom, (prevState) => {
    const targetPrompt = getHistoricalTargetPrompt(
      prevState.past,
      prevState.currentGeneration,
    );
    if (!targetPrompt) return prevState;

    const past = prevState.past.slice(0, -1);
    onSet && onSet(targetPrompt.value);
    return {
      ...prevState,
      past,
      present: targetPrompt,
      future: [...prevState.future, prevState.present],
    };
  });
};

export const nextPrompt = (
  get: Getter,
  set: Setter,
  onSet?: PromptSetHandler,
) => {
  if (get(promptAtom).future.length === 0) {
    return;
  }

  set(promptAtom, (prevState) => {
    const targetPrompt = getHistoricalTargetPrompt(
      prevState.future,
      prevState.currentGeneration,
    );
    if (!targetPrompt) return prevState;

    const future = prevState.future.slice(0, -1);
    onSet && onSet(targetPrompt.value);
    return {
      ...prevState,
      past: [...prevState.past, prevState.present],
      present: targetPrompt,
      future,
    };
  });
};
