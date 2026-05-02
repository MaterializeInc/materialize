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

import { connectorClustersColumns } from "~/api/materialize/cluster/useConnectorClusters";
import { connectionsFilteredColumns } from "~/api/materialize/connection/useConnections";
import { useSchemasColumns } from "~/api/materialize/useSchemas";
import { buildUseSqlQueryHandler } from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import {
  isValidForm,
  renderComponent,
  selectReactSelectOption,
} from "~/test/utils";
import { assert } from "~/util";

import { useSourceForm } from "./forms";
import NewKafkaSourceForm from "./NewKafkaSourceForm";

const FormComponent = () => {
  const form = useSourceForm();
  return <NewKafkaSourceForm form={form} />;
};

async function fillRequiredFormFields(user: UserEvent) {
  const sourceNameInput = screen.getByLabelText("Name");
  await user.type(sourceNameInput, "kafka_source");

  const schemaInput = screen.getByTestId("source-schema");
  await selectReactSelectOption(schemaInput, "public");

  const clusterInput = screen.getByTestId("cluster-selector");
  await selectReactSelectOption(clusterInput, "default");

  const topicInput = screen.getByLabelText("Topic");
  await user.type(topicInput, "test-topic");
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
      // useConnectorClusters
      buildUseSqlQueryHandler({
        type: "SELECT" as const,
        columns: connectorClustersColumns,
        rows: [["u1", "default"]],
      }),
      // useConnectionsFiltered
      buildUseSqlQueryHandler({
        type: "SELECT" as const,
        columns: connectionsFilteredColumns,
        rows: [
          ["u1", "kafka_connection", "public", "materialize", "kafka"],
          // This is a hack, we don't have a good way to supply different results for the
          // kafka connections and the CSR connections
          [
            "u2",
            "csr_connection",
            "public",
            "materialize",
            "confluent-schema-registry",
          ],
        ],
      }),
    );
  });

  it("default form validates successfully", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent />);
    expect(await screen.findByText("General")).toBeVisible();

    await fillRequiredFormFields(user);
    const csrInput = screen.getByTestId("csr-connection");
    await selectReactSelectOption(csrInput, "csr_connection");
    expect(isValidForm()).toBeTruthy();
  });

  it("CSR field is hidden when a registry isn't required", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent />);
    expect(await screen.findByText("General")).toBeVisible();

    await fillRequiredFormFields(user);
    const keyInput = screen.getByTestId("key-format-selector");
    await selectReactSelectOption(keyInput, "Text");
    const valueInput = screen.getByTestId("value-format-selector");
    await selectReactSelectOption(valueInput, "JSON");
    expect(isValidForm()).toBeTruthy();
    expect(screen.queryByTestId("csr-connection")).not.toBeInTheDocument();
  });

  it("CSR field is required", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent />);
    expect(await screen.findByText("General")).toBeVisible();

    await fillRequiredFormFields(user);
    const csrInput = screen.getByTestId("csr-connection");
    // Touch the selection field and change focus so visible validation occurrs
    const csrRawInput = csrInput.querySelector("input");
    assert(csrRawInput);
    await userEvent.click(csrRawInput);
    await userEvent.click(screen.getByLabelText("Topic"));
    expect(csrInput).toBeVisible();
    expect(isValidForm()).toBeFalsy();
  });
});
