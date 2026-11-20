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

const rowOwnedBy = (ownerId: string) =>
  ({ original: { cluster: { ownerId } } }) as Row<{
    cluster: { ownerId: string };
  }>;

const keeps = (ownerId: string, selected: string[]) =>
  ownerFilterFn(rowOwnedBy(ownerId), OWNER_COLUMN_ID, selected);

describe("ownerFilterFn", () => {
  it("keeps a row whose owner is selected", () => {
    expect(keeps("u1", ["u1"])).toBe(true);
  });

  it("drops a row whose owner is not selected", () => {
    expect(keeps("u2", ["u1"])).toBe(false);
  });

  it("keeps a row matching any one of several selected owners", () => {
    expect(keeps("u2", ["u1", "u2"])).toBe(true);
    expect(keeps("u3", ["u1", "u2"])).toBe(false);
  });

  it("matches the whole id, not a part of it", () => {
    expect(keeps("u12", ["u1"])).toBe(false);
  });

  it("filters nothing when the selection is empty or absent", () => {
    expect(keeps("u1", [])).toBe(true);
    expect(
      ownerFilterFn(
        rowOwnedBy("u1"),
        OWNER_COLUMN_ID,
        undefined as unknown as string[],
      ),
    ).toBe(true);
  });
});

describe("ownerFilterFromUrl", () => {
  it("reads the roles' ids, without repeats", () => {
    expect(ownerFilterFromUrl(["u2", "u1", "u2"])).toEqual(["u2", "u1"]);
  });

  it("reads no filter from a URL that names no role", () => {
    expect(ownerFilterFromUrl([])).toBeUndefined();
    expect(ownerFilterFromUrl([""])).toBeUndefined();
  });

  it("keeps a role it can't check, so its chip can remove it", () => {
    expect(ownerFilterFromUrl(["u404"])).toEqual(["u404"]);
  });
});

describe("ownerFilterLabel", () => {
  it("names the column and the role", () => {
    expect(ownerFilterLabel("ana")).toBe("Owner: ana");
  });
});
