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

import { connectorClustersColumns } from "~/api/materialize/cluster/useConnectorClusters";
import { useSchemasColumns } from "~/api/materialize/useSchemas";
import { buildUseSqlQueryHandler } from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import {
  isValidForm,
  renderComponent,
  selectReactSelectOption,
} from "~/test/utils";

import { DatabaseTypeProp } from "./constants";
import { useDatabaseSourceForm } from "./forms";
import NewDatabaseSourceForm, {
  TableNameArrayInput,
} from "./NewDatabaseSourceForm";

const FormComponent = ({ databaseType }: DatabaseTypeProp) => {
  const form = useDatabaseSourceForm(databaseType);
  return <NewDatabaseSourceForm form={form} databaseType={databaseType} />;
};

const TableComponent = ({ databaseType }: DatabaseTypeProp) => {
  const form = useDatabaseSourceForm(databaseType);
  return <TableNameArrayInput form={form} databaseType={databaseType} />;
};

describe("NewPostgresSourceForm", () => {
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
    );
  });

  it("validates Postgres successfully", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent databaseType="postgres" />);
    expect(await screen.findByText("General")).toBeVisible();

    const sourceNameInput = screen.getByLabelText(/Name/);
    await user.type(sourceNameInput, "pg_source");
    const clusterInput = screen.getByTestId("cluster-selector");
    await selectReactSelectOption(clusterInput, "default");
    const publicationInput = screen.getByLabelText(/Publication/);
    await user.type(publicationInput, "mz_source");
    await user.click(screen.getByLabelText("For all tables"));
    expect(isValidForm()).toBeTruthy();
  });

  it("validates MySQL successfully", async () => {
    const user = userEvent.setup();
    renderComponent(<FormComponent databaseType="mysql" />);
    expect(await screen.findByText("General")).toBeVisible();

    const sourceNameInput = screen.getByLabelText(/Name/);
    await user.type(sourceNameInput, "my_source");
    const clusterInput = screen.getByTestId("cluster-selector");
    await selectReactSelectOption(clusterInput, "default");
    const publicationInput = screen.queryByLabelText("Publication");
    expect(publicationInput).not.toBeInTheDocument();
    await user.click(screen.getByLabelText("For all tables"));
    expect(isValidForm()).toBeTruthy();
  });

  it("presents a table name array for Postgres", async () => {
    const user = userEvent.setup();
    renderComponent(<TableComponent databaseType="postgres" />);
    expect(await screen.findByText("Add table")).toBeVisible();

    const schemaNameInput = screen.queryByPlaceholderText("schema name");
    expect(schemaNameInput).not.toBeInTheDocument();

    const addButton = screen.getByText("Add table");
    const nameInput1 = screen.getByPlaceholderText("table name");
    await user.type(nameInput1, "table1");
    expect(isValidForm()).toBeTruthy();

    await user.click(addButton);

    const inputs = Array.from(document.querySelectorAll("input"));
    expect(inputs).toHaveLength(4); // 2 rows of 2 elements
    const removeButtons = screen.getAllByTitle("Remove table");
    expect(removeButtons[0]).not.toBeVisible();
    expect(removeButtons[1]).toBeVisible();

    const nameInput2 = screen.getAllByPlaceholderText("table name")[1];
    await user.type(nameInput2, "table2");

    expect(isValidForm()).toBeTruthy();

    await user.clear(nameInput2);
    await user.type(nameInput2, "table1");
    expect(isValidForm()).toBeFalsy();
  });

  it("presents a table name array for MySQL", async () => {
    const user = userEvent.setup();
    renderComponent(<TableComponent databaseType="mysql" />);
    expect(await screen.findByText("Add table")).toBeVisible();

    const schemaNameInput = screen.queryByPlaceholderText("schema name");
    expect(schemaNameInput).toBeInTheDocument();

    const addButton = screen.getByText("Add table");
    const schemaInput1 = screen.getByPlaceholderText("schema name");
    const nameInput1 = screen.getByPlaceholderText("table name");
    await user.type(schemaInput1, "schema1");
    await user.type(nameInput1, "table1");
    expect(isValidForm()).toBeTruthy();

    await user.click(addButton);

    const inputs = Array.from(document.querySelectorAll("input"));
    expect(inputs).toHaveLength(6); // 2 rows of 3 elements
    const removeButtons = screen.getAllByTitle("Remove table");
    expect(removeButtons[0]).not.toBeVisible();
    expect(removeButtons[1]).toBeVisible();

    const schemaInput2 = screen.getAllByPlaceholderText("schema name")[1];
    const nameInput2 = screen.getAllByPlaceholderText("table name")[1];
    await user.type(schemaInput2, "schema2");
    await user.type(nameInput2, "table1");

    expect(isValidForm()).toBeTruthy();

    await user.clear(schemaInput2);
    await user.type(schemaInput2, "schema1");
    expect(isValidForm()).toBeFalsy();
  });
});
