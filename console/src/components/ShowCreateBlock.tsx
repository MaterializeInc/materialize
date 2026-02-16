// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { prettyStr } from "@materializeinc/sql-pretty";
import * as Sentry from "@sentry/react";
import React from "react";

import { PermissionError } from "~/api/materialize/DatabaseError";
import { ShowCreateObjectType } from "~/api/materialize/showCreate";
import Alert from "~/components/Alert";
import { useShowCreate } from "~/queries/showCreate";

import { ExpandableCodeBlock } from "./ExpandableCodeBlock";
import { ExpectedErrorBoundary } from "./ExpectedErrorBoundary";

export interface ShowCreateBlockProps {
  databaseName: string | null;
  schemaName: string;
  name: string;
  objectType: ShowCreateObjectType;
  isSourceTable?: boolean;
}

function tryPrettyStr(sql: string, width: number) {
  try {
    return prettyStr(sql, width);
  } catch (e) {
    Sentry.captureException(e);
    return sql;
  }
}

export const ShowCreateBlock = (props: ShowCreateBlockProps) => {
  if (!props.databaseName) return null;

  return (
    <ExpectedErrorBoundary
      onError={(error) => {
        // Rethrow anything besides a permission error
        if (!(error instanceof PermissionError)) {
          throw error;
        }
      }}
      fallback={
        <Alert
          width="100%"
          variant="info"
          message="You don't have usage permission on this schema."
        />
      }
    >
      <ShowCreateBlockInner {...props} />
    </ExpectedErrorBoundary>
  );
};

export const ShowCreateBlockInner = ({
  databaseName,
  schemaName,
  name,
  objectType,
  isSourceTable,
}: ShowCreateBlockProps) => {
  const showCreateParams = React.useMemo(
    () => ({
      objectType,
      object: { databaseName, schemaName, name },
      isSourceTable,
    }),
    [databaseName, name, schemaName, objectType, isSourceTable],
  );
  const { data: showCreate } = useShowCreate(showCreateParams);

  return <ExpandableCodeBlock text={tryPrettyStr(showCreate.sql, 100)} />;
};
