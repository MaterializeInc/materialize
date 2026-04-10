// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { MzDataType } from "~/api/materialize/types";

import formatRows from "./formatRows";

const mockTimestamp = 1703005098935;

const mockTimestampDateStringWithoutTz = "2023-12-19 16:58:18.935";
const mockTimestampDateStringWithTz = `${mockTimestampDateStringWithoutTz}+00`;

describe("formatRows", () => {
  it("should format multiple rows correctly", () => {
    const columns = [
      {
        name: "name",
        type_oid: MzDataType.text,
        type_len: 0,
        type_mod: 0,
      },
      {
        name: "age",
        type_oid: MzDataType.int8,
        type_len: 0,
        type_mod: 0,
      },
      {
        name: "timestamp",
        type_oid: MzDataType.timestamp,
        type_len: 0,
        type_mod: 0,
      },
    ];
    const rows = [
      ["Steve", 10, mockTimestamp],
      ["Stacy", 20, mockTimestamp],
    ];

    const formattedRows = formatRows(columns, rows);

    expect(formattedRows).toEqual([
      ["Steve", "10", mockTimestampDateStringWithoutTz],
      ["Stacy", "20", mockTimestampDateStringWithoutTz],
    ]);
  });

  describe("timestamps", () => {
    it("should format the timestamp type correctly", () => {
      const columns = [
        {
          name: "timestamp",
          type_oid: MzDataType.timestamp,
          type_len: 0,
          type_mod: 0,
        },
      ];
      const rows = [[mockTimestamp]];

      const formattedRows = formatRows(columns, rows);

      expect(formattedRows).toEqual([[mockTimestampDateStringWithoutTz]]);
    });

    it("should format the _timestamp type correctly", () => {
      const columns = [
        {
          name: "timestamps",
          type_oid: MzDataType._timestamp,
          type_len: 0,
          type_mod: 0,
        },
      ];
      const rows = [[[mockTimestamp, mockTimestamp]]];

      const formattedRows = formatRows(columns, rows);

      expect(formattedRows).toEqual([
        [
          `{"${mockTimestampDateStringWithoutTz}","${mockTimestampDateStringWithoutTz}"}`,
        ],
      ]);
    });

    it("should format the timestamptz type correctly", () => {
      const columns = [
        {
          name: "timestamptz",
          type_oid: MzDataType.timestamptz,
          type_len: 0,
          type_mod: 0,
        },
      ];
      const rows = [[mockTimestamp]];

      const formattedRows = formatRows(columns, rows);

      expect(formattedRows).toEqual([[mockTimestampDateStringWithTz]]);
    });

    it("should trim trailing zeros in the fractional seconds", () => {
      const columns = [
        {
          name: "timestamps",
          type_oid: MzDataType.timestamp,
          type_len: 0,
          type_mod: 0,
        },
      ];
      const rows = [[1703005098000]];

      const formattedRows = formatRows(columns, rows);

      expect(formattedRows).toEqual([["2023-12-19 16:58:18"]]);
    });

    it("should format the _timestamptz type correctly", () => {
      const columns = [
        {
          name: "list of timestamptz",
          type_oid: MzDataType._timestamptz,
          type_len: 0,
          type_mod: 0,
        },
      ];
      const rows = [[[mockTimestamp, mockTimestamp]]];

      const formattedRows = formatRows(columns, rows);

      expect(formattedRows).toEqual([
        [
          `{"${mockTimestampDateStringWithTz}","${mockTimestampDateStringWithTz}"}`,
        ],
      ]);
    });

    it("should handle incorrect timestamp values", () => {
      const columns = [
        {
          name: "timestamp",
          type_oid: MzDataType.timestamp,
          type_len: 0,
          type_mod: 0,
        },
      ];
      const rows = [["Jane Smith"]];

      const formattedRows = formatRows(columns, rows);

      expect(formattedRows).toEqual([["Jane Smith"]]);
    });

    it("should not append 'BC' for first AD timestamp", () => {
      const columns = [
        {
          name: "timestamptz",
          type_oid: MzDataType.timestamptz,
          type_len: 0,
          type_mod: 0,
        },
      ];
      const rows = [[`${new Date("0001-01-01T00:00:00Z").getTime()}`]];

      const formattedRows = formatRows(columns, rows);

      expect(formattedRows).toEqual([["0001-01-01 00:00:00+00"]]);
    });

    it("should append 'BC' for first BC timestamp", () => {
      const columns = [
        {
          name: "timestamptz",
          type_oid: MzDataType.timestamptz,
          type_len: 0,
          type_mod: 0,
        },
      ];
      const rows = [[`${new Date("0001-01-01T00:00:00Z").getTime() - 1}`]];

      const formattedRows = formatRows(columns, rows);

      expect(formattedRows).toEqual([["0001-12-31 23:59:59.999+00 BC"]]);
    });
  });

  describe("jsonb", () => {
    it("should format jsonb values correctly", () => {
      const columns = [
        {
          name: "jsonb",
          type_oid: MzDataType.jsonb,
          type_len: 0,
          type_mod: 0,
        },
      ];
      const rows = [["a"], [0.3], [{ b: 1 }], [[1, 2, 3]]];

      const formattedRows = formatRows(columns, rows);

      expect(formattedRows).toEqual([
        ['"a"'],
        ["0.3"],
        ['{"b":1}'],
        ["[1,2,3]"],
      ]);
    });

    it("should format jsonb arrays correctly", () => {
      const columns = [
        {
          name: "_jsonb",
          type_oid: MzDataType._jsonb,
          type_len: 0,
          type_mod: 0,
        },
      ];
      const rows = [[[{ a: 1 }, { b: 2 }]]];

      const formattedRows = formatRows(columns, rows);

      expect(formattedRows).toEqual([['{"{\\"a\\":1}","{\\"b\\":2}"}']]);
    });
  });

  describe("arrays", () => {
    it("should format string arrays with weird characters correctly", () => {
      const columns = [
        {
          name: "_text",
          type_oid: MzDataType._text,
          type_len: 0,
          type_mod: 0,
        },
      ];
      const rows = [[["a", ",", "b", "\\", '"', '"c"']]];

      const formattedRows = formatRows(columns, rows);

      expect(formattedRows).toEqual([['{a,",",b,"\\\\","\\"","\\"c\\""}']]);
    });

    it("should format nested arrays correctly", () => {
      const columns = [
        {
          name: "_int4",
          type_oid: MzDataType._int4,
          type_len: 0,
          type_mod: 0,
        },
      ];
      const rows = [
        [
          [
            [1, 2],
            [3, 4],
          ],
        ],
      ];

      const formattedRows = formatRows(columns, rows);

      expect(formattedRows).toEqual([["{{1,2},{3,4}}"]]);
    });
  });
});
