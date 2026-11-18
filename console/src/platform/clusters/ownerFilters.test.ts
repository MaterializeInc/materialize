// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Row } from "@tanstack/react-table";

import {
  OWNER_COLUMN_ID,
  ownerFilterFn,
  ownerFilterFromUrl,
  ownerFilterLabel,
} from "./ownerFilters";

/** A row reporting `owner` in the owner column, which is all the filter reads. */
const rowOwnedBy = (owner: string | null) =>
  ({
    getValue: () => owner,
  }) as unknown as Row<unknown>;

const keeps = (owner: string | null, selected: string[]) =>
  ownerFilterFn(rowOwnedBy(owner), OWNER_COLUMN_ID, selected);

describe("ownerFilterFn", () => {
  it("keeps a row whose owner is selected", () => {
    expect(keeps("ana", ["ana"])).toBe(true);
  });

  it("drops a row whose owner is not selected", () => {
    expect(keeps("ben", ["ana"])).toBe(false);
  });

  it("keeps a row matching any one of several selected owners", () => {
    expect(keeps("ben", ["ana", "ben"])).toBe(true);
    expect(keeps("cy", ["ana", "ben"])).toBe(false);
  });

  it("matches the whole name, not a part of it", () => {
    expect(keeps("ana_dev", ["ana"])).toBe(false);
  });

  it("filters nothing when the selection is empty or absent", () => {
    expect(keeps("ana", [])).toBe(true);
    expect(keeps(null, [])).toBe(true);
    expect(
      ownerFilterFn(
        rowOwnedBy("ana"),
        OWNER_COLUMN_ID,
        undefined as unknown as string[],
      ),
    ).toBe(true);
  });

  it("keeps a cluster whose owner isn't known yet, as while the roles load", () => {
    expect(keeps(null, ["ana"])).toBe(true);
  });
});

describe("ownerFilterFromUrl", () => {
  it("reads the named roles, without repeats", () => {
    expect(ownerFilterFromUrl(["ben", "ana", "ben"])).toEqual(["ben", "ana"]);
  });

  it("reads no filter from a URL that names no role", () => {
    expect(ownerFilterFromUrl([])).toBeUndefined();
    expect(ownerFilterFromUrl([""])).toBeUndefined();
  });

  it("keeps a role it can't check, so its chip can remove it", () => {
    expect(ownerFilterFromUrl(["dropped_role"])).toEqual(["dropped_role"]);
  });
});

describe("ownerFilterLabel", () => {
  it("names the column and the role", () => {
    expect(ownerFilterLabel("ana")).toBe("Owner: ana");
  });
});
