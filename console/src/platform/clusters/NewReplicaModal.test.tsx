// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { render, screen, waitFor } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import React, { ReactElement } from "react";

import { buildSqlQueryHandler } from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { createProviderWrapper } from "~/test/utils";

import NewReplicaModal from "./NewReplicaModal";

const Wrapper = await createProviderWrapper();

const renderComponent = (element: ReactElement) => {
  return render(<Wrapper>{element}</Wrapper>);
};

describe("NewReplicaForm", () => {
  it("creates a new replica", async () => {
    server.use(buildSqlQueryHandler([{ type: "CREATE" as const }]));
    const user = userEvent.setup();
    const closeMock = vi.fn();
    const submitMock = vi.fn();
    renderComponent(
      <NewReplicaModal
        clusterName="default"
        isOpen
        onClose={closeMock}
        onSubmit={submitMock}
      />,
    );

    const nameInput = await screen.findByLabelText("Name");
    await user.type(nameInput, "new_replica");

    await user.click(screen.getByText("Create replica"));

    await waitFor(() => expect(submitMock).toHaveBeenCalled());
  });

  it("requires a name", async () => {
    server.use(buildSqlQueryHandler([{ type: "CREATE" as const }]));
    const user = userEvent.setup();
    const closeMock = vi.fn();
    const submitMock = vi.fn();
    renderComponent(
      <NewReplicaModal
        clusterName="default"
        isOpen
        onClose={closeMock}
        onSubmit={submitMock}
      />,
    );

    await user.click(await screen.findByText("Create replica"));

    expect(screen.getByText("Name is required.")).toBeVisible();
  });
});
