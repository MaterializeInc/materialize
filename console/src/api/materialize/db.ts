// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  DummyDriver,
  Kysely,
  ParseJSONResultsPlugin,
  PostgresAdapter,
  PostgresIntrospector,
  PostgresQueryCompiler,
} from "kysely";

import type { DB as MaterializeSchema } from "~/types/materialize.d.ts";

export const createQueryBuilder = () => {
  return new Kysely<MaterializeSchema>({
    plugins: [new ParseJSONResultsPlugin()],
    dialect: {
      createAdapter() {
        return new PostgresAdapter();
      },
      createDriver() {
        // Kysely requires a database driver, but we decided to just use it as a query builder, rather than implement an HTTP driver
        // Specifically the driver doesn't have support for query cancellation, which we want.
        return new DummyDriver();
      },
      createIntrospector(datbase: Kysely<unknown>) {
        return new PostgresIntrospector(datbase);
      },
      createQueryCompiler() {
        return new PostgresQueryCompiler();
      },
    },
  });
};

export const queryBuilder = createQueryBuilder();
