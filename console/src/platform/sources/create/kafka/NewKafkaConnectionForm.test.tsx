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

import { useConnectionForm } from "./forms";
import NewKafkaConnectionForm, {
  BrokerSection,
} from "./NewKafkaConnectionForm";

const FormComponent = () => {
  const form = useConnectionForm();
  return (
    <NewKafkaConnectionForm
      form={form}
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

const BrokerComponent = () => {
  const form = useConnectionForm();
  return <BrokerSection form={form} />;
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

async function fillRequiredFormFields(user: UserEvent) {
  const connectionNameInput = screen.getByLabelText("Name");
  await user.type(connectionNameInput, "kafka_conn");

  const schemaInput = screen.getByTestId("connection-schema");
  await selectReactSelectOption(schemaInput, "public");

  const brokerInput = screen.getByLabelText("Broker 1");
  await user.type(brokerInput, "broker.example.com:9092");
}

describe("NewKafkaConnectionForm", () => {
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
    renderComponent(<FormComponent />);
    expect(await screen.findByText("Select a connection")).toBeVisible();
    const connectionSelection = screen.getByTestId("connection-selection");
    expect(connectionSelection).toBeVisible();
    await selectReactSelectOption(connectionSelection, "connection_1");
    expect(isValidForm()).toBeTruthy();
  });

  it("displays the connection form when the 'New' option is clicked", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent />);
    await loadNewForm(user);
    expect(await screen.findByText("Configure brokers")).toBeVisible();
    expect(await screen.findByText("Authentication method")).toBeVisible();
  });

  it("can fill out the basic form without authentication", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent />);
    await loadNewForm(user);
    await fillRequiredFormFields(user);
    const noneRadio = screen.getByLabelText("Authentication Mode: none");
    await user.click(noneRadio);
    expect(isValidForm()).toBeTruthy();
  });

  it("can fill out the basic form with SASL authentication", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent />);
    await loadNewForm(user);
    await fillRequiredFormFields(user);
    const saslRadio = screen.getByLabelText("Authentication Mode: sasl");
    await user.click(saslRadio);
    const mechanismInput = screen.getByTestId("sasl-mechanism-selection");
    await selectReactSelectOption(mechanismInput, "SCRAM-SHA-256");
    const userInput = screen.getByLabelText("Username");
    await user.type(userInput, "test_user");
    const passwordInput = screen.getByTestId("sasl-password");
    await selectReactSelectOption(passwordInput, "materialize.public.secret_1");
    const sslCAInput = screen.getByTestId("authentication-ssl-ca");
    await selectReactSelectOption(sslCAInput, "materialize.public.secret_1");
    expect(isValidForm()).toBeTruthy();
  });

  it("can fill out the basic form with SSL authentication", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent />);
    await loadNewForm(user);
    await fillRequiredFormFields(user);
    const saslRadio = screen.getByLabelText("Authentication Mode: ssl");
    await user.click(saslRadio);
    const sslKeyInput = screen.getByTestId("authentication-ssl-key");
    await selectReactSelectOption(sslKeyInput, "materialize.public.secret_1");
    const sslCertInput = screen.getByTestId("authentication-ssl-certificate");
    await selectReactSelectOption(sslCertInput, "materialize.public.secret_1");
    const sslCAInput = screen.getByTestId("authentication-ssl-ca");
    await selectReactSelectOption(sslCAInput, "materialize.public.secret_1");
    expect(isValidForm()).toBeTruthy();
  });

  it("enforces broker uniqueness", async () => {
    const user = userEvent.setup();
    renderComponent(<BrokerComponent />);
    expect(await screen.findByText("Configure brokers")).toBeVisible();
    const addButton = screen.getByText("Add broker");
    expect(addButton).toBeDisabled();

    const brokerInput1 = screen.getByLabelText("Broker 1");
    await user.type(brokerInput1, "broker1.example.com:9092");
    expect(isValidForm()).toBeTruthy();

    expect(addButton).toBeEnabled();
    await user.click(addButton);
    const brokerInput2 = screen.getByLabelText("Broker 2");
    await user.type(brokerInput2, "broker2.example.com:9092");
    expect(isValidForm()).toBeTruthy();

    await user.clear(brokerInput2);
    await user.type(brokerInput2, "broker1.example.com:9092");
    await user.click(addButton);
    expect(isValidForm()).toBeFalsy();
  });
});
