// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useForm } from "react-hook-form";

import { DatabaseType } from "./constants";
import { DatabaseConnectionFormState } from "./NewDatabaseConnectionForm";
import { DatabaseSourceFormState } from "./NewDatabaseSourceForm";

export function useDatabaseConnectionForm(databaseType?: DatabaseType) {
  // SQL Server uses different SSL mode names than Postgres/MySQL
  const defaultSslMode = databaseType === "sql-server" ? "required" : "require";

  return useForm<DatabaseConnectionFormState>({
    defaultValues: {
      connectionAction: "existing",
      name: "",
      host: "",
      port: "",
      sslAuthentication: false,
      sslMode: defaultSslMode,
    },
    mode: "onTouched",
  });
}

export function useDatabaseSourceForm(databaseType: DatabaseType) {
  return useForm<DatabaseSourceFormState>({
    defaultValues: {
      name: "",
      cluster: null,
      publication: "",
      allTables: false,
      tables: [
        {
          name: "",
          alias: "",
          schemaName: databaseType === "mysql" ? "" : undefined,
        },
      ],
    },
    mode: "onTouched",
  });
}
