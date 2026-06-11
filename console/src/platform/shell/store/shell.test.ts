// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SUBSCRIBE_METADATA_COLUMNS } from "~/api/materialize/SubscribeManager";
import { Column } from "~/api/materialize/types";

import {
  mergeMzDiffs,
  setStoredActiveTutorial,
  SHELL_ACTIVE_TUTORIAL,
} from "./shell";

describe("shell", () => {
  describe("setStoredActiveTutorial", () => {
    afterEach(() => {
      window.localStorage.removeItem(SHELL_ACTIVE_TUTORIAL);
    });

    it("persists an academy selection", () => {
      setStoredActiveTutorial("academy");
      expect(window.localStorage.getItem(SHELL_ACTIVE_TUTORIAL)).toBe(
        "academy",
      );
    });

    it("persists a quickstart selection", () => {
      setStoredActiveTutorial("quickstart");
      expect(window.localStorage.getItem(SHELL_ACTIVE_TUTORIAL)).toBe(
        "quickstart",
      );
    });
  });

  describe("mergeMzDiffs", () => {
    it("should return the input if isStreamingResult is false", () => {
      const commandResult = {
        isStreamingResult: false,
        hasRows: true,
        cols: [{ name: "column1" } as Column],
        rows: [[]],
        notices: [],
      };

      const result = mergeMzDiffs(commandResult);

      expect(result).toEqual(commandResult);
    });

    it("should return an empty list for rows if commandResult.rows is undefined", () => {
      const commandResult = {
        isStreamingResult: true,
        hasRows: true,
        cols: [
          { name: "column1" },
          { name: "column2" },
          { name: SUBSCRIBE_METADATA_COLUMNS.mz_diff },
        ] as Column[],
        notices: [],
      };

      const expected = {
        ...commandResult,
        cols: [{ name: "column1" }, { name: "column2" }],
        rows: [],
      };

      const result = mergeMzDiffs(commandResult);

      expect(result).toEqual(expected);
    });

    it("should return the input if isStreamingResult is true and does not contain an mz_diff column", () => {
      const commandResult = {
        isStreamingResult: true,
        hasRows: true,
        cols: [{ name: "column1" }] as Column[],
        rows: [[]],
        notices: [],
      };

      const result = mergeMzDiffs(commandResult);

      expect(result).toEqual(commandResult);
    });

    it("should merge mz_diff values correctly", () => {
      const commandResult = {
        isStreamingResult: true,
        hasRows: true,
        cols: [
          { name: "column1" },
          { name: "column2" },
          { name: SUBSCRIBE_METADATA_COLUMNS.mz_diff },
        ] as Column[],
        rows: [
          ["a.key", "a.val", "2"],
          ["b.key", "b.val", "1"],
          ["a.key", "a.val", "-1"],
        ],
        notices: [],
      };

      const expected = {
        ...commandResult,
        cols: [{ name: "column1" }, { name: "column2" }],
        rows: [
          ["a.key", "a.val"],
          ["b.key", "b.val"],
        ],
      };

      const result = mergeMzDiffs(commandResult);

      expect(result).toEqual(expected);
    });

    it("should merge mz_diff values correctly for duplicate rows", () => {
      const commandResult = {
        isStreamingResult: true,
        hasRows: true,
        cols: [
          { name: "column1" },
          { name: "column2" },
          { name: SUBSCRIBE_METADATA_COLUMNS.mz_diff },
        ] as Column[],
        rows: [
          ["a.key", "a.val", "2"],
          ["b.key", "b.val", "1"],
          ["a.key", "a.val", "-2"],
          ["a.key", "a.val", "3"],
        ],
        notices: [],
      };

      const expected = {
        ...commandResult,
        cols: [{ name: "column1" }, { name: "column2" }],
        rows: [
          ["b.key", "b.val"],
          ["a.key", "a.val"],
          ["a.key", "a.val"],
          ["a.key", "a.val"],
        ],
      };

      const result = mergeMzDiffs(commandResult);

      expect(result).toEqual(expected);
    });

    it("should filter metadata columns", () => {
      const commandResult = {
        isStreamingResult: true,
        hasRows: true,
        cols: [
          { name: SUBSCRIBE_METADATA_COLUMNS.mz_diff },
          { name: SUBSCRIBE_METADATA_COLUMNS.mz_timestamp },
          { name: SUBSCRIBE_METADATA_COLUMNS.mz_progressed },
        ] as Column[],
        rows: [],
        notices: [],
      };

      const expected = {
        ...commandResult,
        cols: [],
        rows: [],
      };

      const result = mergeMzDiffs(commandResult);

      expect(result).toEqual(expected);
    });

    it("should allow columns with the same name as metadata columns if they are duplicates", () => {
      const commandResult = {
        isStreamingResult: true,
        hasRows: true,
        cols: [
          { name: SUBSCRIBE_METADATA_COLUMNS.mz_diff },
          { name: SUBSCRIBE_METADATA_COLUMNS.mz_timestamp },
          { name: SUBSCRIBE_METADATA_COLUMNS.mz_progressed },
          { name: "column1" },
          { name: SUBSCRIBE_METADATA_COLUMNS.mz_timestamp },
        ] as Column[],
        rows: [],
        notices: [],
      };

      const expected = {
        ...commandResult,
        cols: [
          { name: "column1" },
          { name: SUBSCRIBE_METADATA_COLUMNS.mz_timestamp },
        ],
        rows: [],
      };

      const result = mergeMzDiffs(commandResult);

      expect(result).toEqual(expected);
    });
  });
});
