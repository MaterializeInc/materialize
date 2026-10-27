// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { Navigate, useSearchParams } from "react-router-dom";
import { output, ZodType } from "zod";

/**
 * Validates the current location's search parameters against the provided Zod schema.
 * If the search parameters are invalid, redirects to the current location without search params.
 *
 * @param schema - The Zod schema to validate the search params against
 * @param mapSearchParamsToSchema - A function that takes in a URLSearchParams object and returns an object that will be validated against the provided Zod schema
 * @param children - A render prop that provides the validated schema data from the search params
 *
 */
const StripSearchIfInvalidZodSchema: <Schema extends ZodType>(props: {
  schema: Schema;
  mapSearchParamsToSchema: (searchParams: URLSearchParams) => unknown;
  children: (validatedSearchParamData: output<Schema>) => React.ReactNode;
}) => React.ReactNode = ({ schema, mapSearchParamsToSchema, children }) => {
  const [searchParams] = useSearchParams();

  const searchParamValues = mapSearchParamsToSchema(searchParams);

  const parsedUrlSchema = schema.safeParse(searchParamValues);

  if (!parsedUrlSchema.success) {
    return <Navigate to="." replace />;
  }

  return children(parsedUrlSchema.data);
};

export default StripSearchIfInvalidZodSchema;
