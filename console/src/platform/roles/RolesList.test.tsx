// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import React from "react";

import { allRoles, RoleItem } from "~/store/allRoles";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { renderComponent } from "~/test/utils";

import { RolesList } from "./RolesList";

const defaultRoles: RoleItem[] = [
  {
    roleId: "u1",
    roleName: "admin",
    memberCount: 2,
    ownedObjectsCount: 0,
  },
  {
    roleId: "u2",
    roleName: "engineer",
    memberCount: 5,
    ownedObjectsCount: 0,
  },
];

describe("RolesList", () => {
  it("renders the list of roles", async () => {
    await renderComponent(<RolesList roles={defaultRoles} />, {
      initializeState: ({ set }) => {
        set(allRoles, mockSubscribeState({ data: defaultRoles }));
      },
    });

    expect(await screen.findByText("admin")).toBeVisible();
    expect(await screen.findByText("2")).toBeVisible();
    expect(await screen.findByText("engineer")).toBeVisible();
    expect(await screen.findByText("5")).toBeVisible();
  });

  it("handles empty roles list", async () => {
    await renderComponent(<RolesList roles={[]} />, {
      initializeState: ({ set }) => {
        set(allRoles, mockSubscribeState({ data: [] }));
      },
    });

    expect(await screen.findByText("Role name")).toBeVisible();
    expect(screen.queryByText("admin")).not.toBeInTheDocument();
  });
});
