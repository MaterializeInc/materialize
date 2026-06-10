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

import { DatabaseTypeProp } from "./constants";
import { useDatabaseConnectionForm } from "./forms";
import NewDatabaseConnectionForm from "./NewDatabaseConnectionForm";

const FormComponent = ({ databaseType }: DatabaseTypeProp) => {
  const form = useDatabaseConnectionForm(databaseType);
  return (
    <NewDatabaseConnectionForm
      form={form}
      databaseType={databaseType}
      connections={[
        {
          id: "u1",
          databaseName: "materialize",
          name: "connection_1",
          schemaName: "public",
          type: "postgres",
        },
      ]}
    />
  );
};

async function loadNewForm(user: UserEvent) {
  expect(await screen.findByText("Select a connection")).toBeVisible();
  const actions = screen.getByTestId("connection-action");
  expect(actions).toBeVisible();
  const newRadio = screen.getByLabelText("New connection");
  await user.click(newRadio);
  expect(
    await screen.findByText("Configure your connection type"),
  ).toBeVisible();
}

async function _fillSharedRequiredFields(user: UserEvent) {
  const connectionNameInput = screen.getByLabelText(/Name/);
  await user.type(connectionNameInput, "pg_connection");

  const schemaInput = screen.getByTestId("connection-schema");
  await selectReactSelectOption(schemaInput, "public");

  const hostInput = screen.getByLabelText(/Host/);
  await user.type(hostInput, "example.com");

  const userInput = screen.getByLabelText(/User/);
  await user.type(userInput, "test_user");

  const passwordInput = screen.getByTestId("authentication-password");
  await selectReactSelectOption(passwordInput, "materialize.public.secret_1");
}

async function fillPostgresRequiredFields(user: UserEvent) {
  await _fillSharedRequiredFields(user);
  const databaseInput = screen.getByLabelText(/Database/);
  await user.type(databaseInput, "test_database");
}

async function fillMySqlRequiredFields(user: UserEvent) {
  await _fillSharedRequiredFields(user);
}

async function fillSqlServerRequiredFields(user: UserEvent) {
  await _fillSharedRequiredFields(user);
  const databaseInput = screen.getByLabelText(/Database/);
  await user.type(databaseInput, "test_database");
}

describe("NewDatabaseConnectionForm", () => {
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

  it("presents a list of existing connections", async () => {
    renderComponent(<FormComponent databaseType="postgres" />);
    expect(await screen.findByText("Select a connection")).toBeVisible();
    const connectionSelection = screen.getByTestId("connection-selection");
    expect(connectionSelection).toBeVisible();
    await selectReactSelectOption(connectionSelection, "connection_1");
    expect(isValidForm()).toBeTruthy();
  });

  it("displays the connection form when the 'New' option is clicked", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent databaseType="postgres" />);
    await loadNewForm(user);
    expect(await screen.findByText("Connection details")).toBeVisible();
    expect(await screen.findByText("Authentication")).toBeVisible();
  });

  it("can fill out the basic form for Postgres", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent databaseType="postgres" />);
    await loadNewForm(user);
    await fillPostgresRequiredFields(user);
    const sslToggle = screen.getByLabelText("SSL Authentication");
    expect(sslToggle).toBeVisible();
    expect(sslToggle).not.toBeChecked();
    expect(isValidForm()).toBeTruthy();
  });

  it("can fill out the basic form for MySQL", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent databaseType="mysql" />);
    await loadNewForm(user);
    await fillMySqlRequiredFields(user);
    const sslToggle = screen.getByLabelText("SSL Authentication");
    expect(sslToggle).toBeVisible();
    expect(sslToggle).not.toBeChecked();
    expect(isValidForm()).toBeTruthy();
  });

  it("SSL authentication fields can be populated", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent databaseType="postgres" />);
    await loadNewForm(user);
    await fillPostgresRequiredFields(user);

    // SSL enabled
    const sslToggle = screen.getByLabelText("SSL Authentication");
    expect(sslToggle).toBeVisible();
    await user.click(sslToggle);
    expect(sslToggle).toBeChecked();
    const sslCertInput = screen.getByTestId("authentication-ssl-certificate");
    await selectReactSelectOption(sslCertInput, "materialize.public.secret_1");

    const sslKeyInput = screen.getByTestId("authentication-ssl-key");
    await selectReactSelectOption(sslKeyInput, "materialize.public.secret_1");

    // SSL modes
    const sslModeRequire = screen.getByLabelText("SSL Mode: require");
    const sslModeVerifyCA = screen.getByLabelText("SSL Mode: verify-ca");
    const sslModeVerifyFull = screen.getByLabelText("SSL Mode: verify-full");
    expect(sslModeRequire).toBeChecked();
    expect(
      screen.queryByTestId("authentication-ssl-ca"),
    ).not.toBeInTheDocument();
    await user.click(sslModeVerifyCA);
    expect(screen.queryByTestId("authentication-ssl-ca")).toBeVisible();
    await user.click(sslModeVerifyFull);
    const sslCAInput = screen.getByTestId("authentication-ssl-ca");
    expect(sslCAInput).toBeVisible();
    await selectReactSelectOption(sslCAInput, "materialize.public.secret_1");
    expect(isValidForm()).toBeTruthy();
  });

  it("displays the appropriate fields for PostgresSQL", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent databaseType="postgres" />);
    await loadNewForm(user);
    const databaseInput = screen.queryByLabelText(/Database/);
    expect(databaseInput).toBeInTheDocument();
    const portInput = screen.getByLabelText("Port");
    expect(portInput).toHaveProperty("placeholder", "5432");

    const sslToggle = screen.getByLabelText("SSL Authentication");
    expect(sslToggle).toBeVisible();
    await user.click(sslToggle);
    expect(sslToggle).toBeChecked();
  });

  it("displays the appropriate fields for MySQL", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent databaseType="mysql" />);
    await loadNewForm(user);
    const databaseInput = screen.queryByLabelText("Database");
    expect(databaseInput).not.toBeInTheDocument();
    const portInput = screen.getByLabelText("Port");
    expect(portInput).toHaveProperty("placeholder", "3306");

    const sslToggle = screen.getByLabelText("SSL Authentication");
    expect(sslToggle).toBeVisible();
    await user.click(sslToggle);
    expect(sslToggle).toBeChecked();
  });

  it("can fill out the basic form for SQL Server", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent databaseType="sql-server" />);
    await loadNewForm(user);
    await fillSqlServerRequiredFields(user);
    const sslToggle = screen.getByLabelText("SSL Authentication");
    expect(sslToggle).toBeVisible();
    expect(sslToggle).not.toBeChecked();
    expect(isValidForm()).toBeTruthy();
  });

  it("displays the appropriate fields for SQL Server", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent databaseType="sql-server" />);
    await loadNewForm(user);
    const databaseInput = screen.queryByLabelText(/Database/);
    expect(databaseInput).toBeInTheDocument();
    const portInput = screen.getByLabelText("Port");
    expect(portInput).toHaveProperty("placeholder", "1433");

    const sslToggle = screen.getByLabelText("SSL Authentication");
    expect(sslToggle).toBeVisible();
    await user.click(sslToggle);
    expect(sslToggle).toBeChecked();
  });

  it("SQL Server SSL authentication fields can be populated", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent databaseType="sql-server" />);
    await loadNewForm(user);
    await fillSqlServerRequiredFields(user);

    // SSL enabled
    const sslToggle = screen.getByLabelText("SSL Authentication");
    expect(sslToggle).toBeVisible();
    await user.click(sslToggle);
    expect(sslToggle).toBeChecked();
    const sslCertInput = screen.getByTestId("authentication-ssl-certificate");
    await selectReactSelectOption(sslCertInput, "materialize.public.secret_1");

    const sslKeyInput = screen.getByTestId("authentication-ssl-key");
    await selectReactSelectOption(sslKeyInput, "materialize.public.secret_1");

    // SQL Server SSL modes
    const sslModeRequired = screen.getByLabelText("SSL Mode: required");
    const sslModeVerify = screen.getByLabelText("SSL Mode: verify");
    const sslModeVerifyCA = screen.getByLabelText("SSL Mode: verify_ca");
    expect(sslModeRequired).toBeChecked();
    expect(
      screen.queryByTestId("authentication-ssl-ca"),
    ).not.toBeInTheDocument();
    await user.click(sslModeVerify);
    expect(screen.queryByTestId("authentication-ssl-ca")).toBeVisible();
    await user.click(sslModeVerifyCA);
    const sslCAInput = screen.getByTestId("authentication-ssl-ca");
    expect(sslCAInput).toBeVisible();
    await selectReactSelectOption(sslCAInput, "materialize.public.secret_1");
    expect(isValidForm()).toBeTruthy();
  });
});
