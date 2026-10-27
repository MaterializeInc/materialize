// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Button,
  FormControl,
  FormErrorMessage,
  HStack,
  Input,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useAtom } from "jotai";
import React, { useState } from "react";
import { useForm, useWatch } from "react-hook-form";

import { useSqlLazy } from "~/api/materialize";
import ReadOnlyCommandBlock from "~/components/CommandBlock/ReadOnlyCommandBlock";
import { MaterializeTheme } from "~/theme";

import { shellStateAtom } from "./store/shell";

const TutorialSchemaWidget = () => {
  const { colors } = useTheme<MaterializeTheme>();
  const [shellState] = useAtom(shellStateAtom);
  const [formError, setFormError] = useState<string | null>(null);

  const {
    sessionParameters: { cluster },
  } = shellState;

  const { runSql: createSchema, loading } = useSqlLazy({
    queryBuilder: (schemaName: string) => {
      const createSchemaSQL = `CREATE SCHEMA materialize.${schemaName}`;
      const setSchemaSQL = `SET SCHEMA ${schemaName}`;
      return {
        queries: [
          {
            query: createSchemaSQL,
            params: [],
          },
          {
            query: setSchemaSQL,
            params: [],
          },
        ],
        cluster: cluster ?? "mz_catalog_server",
      };
    },
  });

  const performCreateSchema = (newSchema: string) => {
    createSchema(newSchema, {
      onSuccess: () => {
        setFormError(null);
        reset();
      },
      onError: (errorMessage) => {
        setFormError(errorMessage ?? "There was an error creating this schema");
      },
    });
  };

  const { formState, register, control, handleSubmit, reset } = useForm<{
    schemaName: string;
  }>({
    defaultValues: {
      schemaName: "",
    },
    mode: "onChange",
  });

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
   */

  const schemaNameRegex = new RegExp(
    "^[A-Za-z_\u{0080}-\u{FFFF}][A-Za-z0-9_$\u{0080}-\u{FFFF}]*$",
  );

  /**
   * If the schemaName is double-quoted, allow all but the period (.).
   *
   * That is, the following quoted schemas are valid:
   *
   * - "   "
   * - "?cluster=quickstart&database=materialize&search_path=b"
   * - "foo!bar"
   * - "foo'bar"
   *
   * But the following is not valid:
   *
   * "foo.bar"
   *
   * */
  const schemaNameQuotedRegex = new RegExp('^"[^."]+"$');

  const formSchemaName = useWatch({ name: "schemaName", control });

  /**
   * Check that schema name matches pattern.
   */
  const isSchemaNameValid =
    schemaNameRegex.test(formSchemaName) ||
    schemaNameQuotedRegex.test(formSchemaName);

  const createSchemaCommand = `CREATE SCHEMA materialize.${
    isSchemaNameValid ? formSchemaName : "<schemaName>"
  };`;

  return (
    <form
      onSubmit={handleSubmit((formdata) =>
        performCreateSchema(formdata.schemaName),
      )}
    >
      <VStack
        gap={4}
        alignItems="flex-start"
        padding={4}
        rounded="lg"
        border="1px solid"
        borderColor={colors.border.primary}
      >
        <Text textStyle="heading-xs" color={colors.foreground.primary}>
          Enter a schema name
        </Text>
        <FormControl
          isInvalid={(formState.isValid && !isSchemaNameValid) || !!formError}
        >
          <Input
            {...register("schemaName", {
              required: "Schema name is required.",
            })}
            type="string"
            variant="default"
            size="md"
            width="100%"
            placeholder="Enter your schema name"
            data-testid="schema-name-input"
          />
          {!isSchemaNameValid && (
            <FormErrorMessage>Invalid schema name.</FormErrorMessage>
          )}
          {formError && <FormErrorMessage>{formError}</FormErrorMessage>}
        </FormControl>
        <HStack
          width="100%"
          justify="space-between"
          data-testid="schema-command-line"
        >
          <ReadOnlyCommandBlock
            value={createSchemaCommand}
            containerProps={{ overflow: "auto" }}
          />
          <Button
            width="fit-content"
            variant="tertiary"
            paddingX={4}
            size="md"
            isDisabled={loading || !formState.isValid || !isSchemaNameValid}
            type="submit"
            data-testid="schema-name-submit"
            flexShrink="0"
          >
            Create
          </Button>
        </HStack>
      </VStack>
    </form>
  );
};

export default TutorialSchemaWidget;
