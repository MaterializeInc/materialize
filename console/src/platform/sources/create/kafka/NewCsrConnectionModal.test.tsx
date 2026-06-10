// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import { UserEvent, userEvent } from "@testing-library/user-event";
import React from "react";

import { useSchemasColumns } from "~/api/materialize/useSchemas";
import {
  buildSqlQueryHandlerV2,
  buildUseSqlQueryHandler,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { secretQueryKeys } from "~/platform/secrets/queries";
import { secretListColumns } from "~/test/queries";
import {
  isValidForm,
  renderComponent,
  selectReactSelectOption,
} from "~/test/utils";

import { useCsrConnectionForm } from "./forms";
import { NewCsrConnectionForm } from "./NewCsrConnectionModal";

const FormComponent = () => {
  const form = useCsrConnectionForm();
  return <NewCsrConnectionForm form={form} />;
};

async function fillRequiredFormFields(user: UserEvent) {
  const connectionNameInput = screen.getByLabelText("Name");
  await user.type(connectionNameInput, "csr_conn");

  const schemaInput = screen.getByTestId("connection-schema");
  await selectReactSelectOption(schemaInput, "public");

  const urlInput = screen.getByLabelText("Schema Registry URL");
  await user.type(urlInput, "registry.example.com:9092");

  const usernameInput = screen.getByLabelText("Username");
  await user.type(usernameInput, "user");

  const passwordInput = screen.getByTestId("csr-password");
  await selectReactSelectOption(passwordInput, "materialize.public.secret_1");
}

describe("NewCsrConnectionModal", () => {
  beforeEach(() => {
    server.use(
      // useSchemas
      buildUseSqlQueryHandler({
        type: "SELECT" as const,
        columns: useSchemasColumns,
        rows: [["u1", "public", "u1", "materialize"]],
      }),
      // useSecrets
      buildSqlQueryHandlerV2({
        queryKey: secretQueryKeys.list(),
        results: mapKyselyToTabular({
          rows: [
            {
              id: "u1",
              name: "secret_1",
              createdAt: "1695165227439",
              databaseName: "materialize",
              schemaName: "public",
              isOwner: true,
            },
          ],
          columns: secretListColumns,
        }),
      }),
    );
  });

  it("can fill out the basic form", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent />);
    expect(await screen.findByText("Configure connection")).toBeVisible();

    await fillRequiredFormFields(user);
    expect(isValidForm()).toBeTruthy();
  });

  it("can fill out the basic form with SSL Authentication", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent />);
    expect(await screen.findByText("Configure connection")).toBeVisible();

    await fillRequiredFormFields(user);
    await user.click(screen.getByLabelText("Use SSL Authentication"));
    const sslKeyInput = screen.getByTestId("authentication-ssl-key");
    await selectReactSelectOption(sslKeyInput, "materialize.public.secret_1");
    const sslCertInput = screen.getByTestId("authentication-ssl-certificate");
    await selectReactSelectOption(sslCertInput, "materialize.public.secret_1");
    expect(isValidForm()).toBeTruthy();
  });

  it("can fill out the basic form with SSL Authentication and a CA", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent />);
    expect(await screen.findByText("Configure connection")).toBeVisible();

    await fillRequiredFormFields(user);
    await user.click(screen.getByLabelText("Use SSL Authentication"));
    const sslKeyInput = screen.getByTestId("authentication-ssl-key");
    await selectReactSelectOption(sslKeyInput, "materialize.public.secret_1");
    const sslCertInput = screen.getByTestId("authentication-ssl-certificate");
    await selectReactSelectOption(sslCertInput, "materialize.public.secret_1");
    const sslCAInput = screen.getByTestId("authentication-ssl-ca");
    await selectReactSelectOption(sslCAInput, "materialize.public.secret_1");
    expect(isValidForm()).toBeTruthy();
  });
});
