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
import { http, HttpResponse } from "msw";
import React from "react";

import { ExtendedRequest } from "~/api/materialize/types";
import server from "~/api/mocks/server";
import { createProviderWrapper } from "~/test/utils";
import { assert } from "~/util";

import TutorialSchemaWidget from "./TutorialSchemaWidget";

const Wrapper = await createProviderWrapper();

const renderComponent = () => {
  return render(
    <Wrapper>
      <TutorialSchemaWidget />
    </Wrapper>,
  );
};

describe("TutorialSchemaWidget", () => {
  /**
   * Valid schema name:
   *
   * - The first character must be:
   *   - ASCII letter (a-z and A-Z),
   *   - an underscore (_), or
   *   - any non-ASCII character.
   *
   * - The remaining characters can be:
   *
   *   - ASCII letters (a-z and A-Z),
   *   - ASCII numbers (0-9),
   *   - an underscore (_),
   *   - dollar signs ($), or
   *   - non-ASCII characters.
   *
   * -OR-
   *
   * You can bypass above rules by double-quoting the name
   * - But with the exception of the dot (.)  Schema names cannot contain dots.
   *
   */

  it("displays a valid form for valid inputs", async () => {
    renderComponent();
    const user = userEvent.setup();
    const schemaName = screen.getByTestId("schema-name-input");
    const submitBtn = screen.getByTestId("schema-name-submit");
    const line = screen
      .getByTestId("schema-command-line")
      .querySelector(".cm-line");
    assert(line);
    await user.type(schemaName, "my_test_schema1");
    expect(submitBtn).toBeEnabled();
    expect(line.textContent).toEqual(
      "CREATE SCHEMA materialize.my_test_schema1;",
    );
    await user.clear(schemaName);
    await user.type(schemaName, "My_test_schema2");
    expect(submitBtn).toBeEnabled();
    expect(line.textContent).toEqual(
      "CREATE SCHEMA materialize.My_test_schema2;",
    );
    await user.clear(schemaName);
    await user.type(schemaName, "my_test$schema3");
    expect(submitBtn).toBeEnabled();
    expect(line.textContent).toEqual(
      "CREATE SCHEMA materialize.my_test$schema3;",
    );
    await user.clear(schemaName);
    await user.type(schemaName, "my_test$καφέ4");
    expect(submitBtn).toBeEnabled();
    expect(line.textContent).toEqual(
      "CREATE SCHEMA materialize.my_test$καφέ4;",
    );
    await user.clear(schemaName);
    await user.type(schemaName, "_testschema5");
    expect(submitBtn).toBeEnabled();
    expect(line.textContent).toEqual("CREATE SCHEMA materialize._testschema5;");
    await user.clear(schemaName);
    await user.type(schemaName, '"   "');
    expect(submitBtn).toBeEnabled();
    expect(line.textContent).toEqual('CREATE SCHEMA materialize."   ";');
    await user.clear(schemaName);
    await user.type(schemaName, '"f00bar!"');
    expect(submitBtn).toBeEnabled();
    expect(line.textContent).toEqual('CREATE SCHEMA materialize."f00bar!";');
    await user.clear(schemaName);
    await user.type(schemaName, '"$myschema"');
    expect(submitBtn).toBeEnabled();
    expect(line.textContent).toEqual('CREATE SCHEMA materialize."$myschema";');
    await user.clear(schemaName);
    await user.type(schemaName, '"?cluster=quickstart&database=materialize"');
    expect(submitBtn).toBeEnabled();
    expect(line.textContent).toEqual(
      'CREATE SCHEMA materialize."?cluster=quickstart&database=materialize";',
    );
  });

  it("displays an invalid form for invalid inputs", async () => {
    renderComponent();
    const user = userEvent.setup();
    const schemaName = screen.getByTestId("schema-name-input");
    const submitBtn = screen.getByTestId("schema-name-submit");
    const line = screen
      .getByTestId("schema-command-line")
      .querySelector(".cm-line");
    assert(line);
    await user.type(schemaName, "   ");
    expect(submitBtn).toBeDisabled();
    expect(line.textContent).toEqual("CREATE SCHEMA materialize.<schemaName>;");
    await user.clear(schemaName);
    await user.type(schemaName, "5myschema");
    expect(submitBtn).toBeDisabled();
    expect(line.textContent).toEqual("CREATE SCHEMA materialize.<schemaName>;");
    await user.clear(schemaName);
    await user.type(schemaName, "$myschema");
    expect(submitBtn).toBeDisabled();
    expect(line.textContent).toEqual("CREATE SCHEMA materialize.<schemaName>;");
    await user.clear(schemaName);
    await user.type(schemaName, "my-schema");
    expect(submitBtn).toBeDisabled();
    expect(line.textContent).toEqual("CREATE SCHEMA materialize.<schemaName>;");
    await user.clear(schemaName);
    await user.type(schemaName, "my.schema");
    expect(submitBtn).toBeDisabled();
    expect(line.textContent).toEqual("CREATE SCHEMA materialize.<schemaName>;");
    await user.clear(schemaName);
    await user.type(schemaName, '"my.schema"');
    expect(submitBtn).toBeDisabled();
    expect(line.textContent).toEqual("CREATE SCHEMA materialize.<schemaName>;");
  });

  it("submits a correct query", async () => {
    renderComponent();
    server.use(
      http.post("*/api/sql", async (info) => {
        const body = await info.request.clone().json();
        if (body == null) {
          return undefined;
        }
        const { queries: requestQueries }: ExtendedRequest = body;
        if (
          requestQueries.length === 1 &&
          requestQueries[0].query === `CREATE SCHEMA materialize.$1`
        ) {
          return HttpResponse.json({
            results: [
              {
                success: true,
                notices: [],
              },
            ],
          });
        }
      }),
    );
    const user = userEvent.setup();
    const schemaName =
      screen.getByTestId<HTMLInputElement>("schema-name-input");
    const submitBtn = screen.getByTestId("schema-name-submit");
    await user.type(schemaName, "myNewSchema");
    expect(submitBtn).toBeEnabled();
    await user.click(submitBtn);
    await waitFor(() => expect(schemaName.value).toEqual("myNewSchema"));
  });
});
