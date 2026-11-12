// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";

import {
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { renderComponent } from "~/test/utils";

import { roleQueryKeys } from "./queries";
import RoleGraph from "./RoleGraph";

describe("RoleGraph", () => {
  beforeEach(() => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.list(),
        results: mapKyselyToTabular({
          rows: [
            {
              roleId: "u2",
              roleName: "__proto__",
              memberCount: 0,
              ownedObjectsCount: 0,
            },
            {
              roleId: "u3",
              roleName: "cache",
              memberCount: 1,
              ownedObjectsCount: 0,
            },
          ],
        }),
      }),
      buildSqlQueryHandlerV2({
        queryKey: roleQueryKeys.edges(),
        results: mapKyselyToTabular({
          rows: [
            {
              id: "u3",
              roleName: "__proto__",
              memberName: "cache",
              childRoleName: "cache",
              parentName: "__proto__",
              childName: "cache",
              parentId: "u2",
              childId: "u3",
            },
          ],
        }),
      }),
    );
  });

  afterEach(() => {
    Reflect.deleteProperty(Object.prototype, "cache");
  });

  it("renders roles named after Object.prototype keys without writing to it", async () => {
    renderComponent(<RoleGraph />);

    expect(await screen.findByText("__proto__")).toBeVisible();
    expect(screen.getByText("cache")).toBeVisible();
    expect(Object.hasOwn(Object.prototype, "cache")).toBe(false);
  });

  it("selects a related role clicked in the sidebar", async () => {
    const user = userEvent.setup();
    renderComponent(<RoleGraph />);

    // Graph node buttons also name the member count, so an exact name only
    // matches the sidebar's related-role buttons.
    await user.click(await screen.findByRole("button", { name: /^cache/ }));
    await user.click(screen.getByRole("button", { name: "__proto__" }));

    expect(screen.getByRole("button", { name: "cache" })).toBeVisible();
    expect(
      screen.queryByRole("button", { name: "__proto__" }),
    ).not.toBeInTheDocument();
  });
});
