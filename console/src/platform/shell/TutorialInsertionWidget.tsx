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
import { sql } from "kysely";
import React, { useState } from "react";
import { useForm, useWatch } from "react-hook-form";

import {
  buildFullyQualifiedObjectName,
  parseSearchPath,
} from "~/api/materialize";
import { queryBuilder, useSqlLazy } from "~/api/materialize";
import ReadOnlyCommandBlock from "~/components/CommandBlock/ReadOnlyCommandBlock";
import { MaterializeTheme } from "~/theme";

import { shellStateAtom } from "./store/shell";

const TutorialInsertionWidget = () => {
  const { colors } = useTheme<MaterializeTheme>();
  const [shellState] = useAtom(shellStateAtom);
  const [formError, setFormError] = useState<string | null>(null);

  const {
    sessionParameters: { cluster, database, search_path },
  } = shellState;

  // The active schema is the first extent schema in the search_path. We are not
  // yet able to cleanly track this on the client side, so make a best-effort attempt.
  const [schema] = parseSearchPath(search_path);

  const { runSql: insertflipperAccount, loading } = useSqlLazy({
    queryBuilder: (accountId: number) => {
      const qualifiedIdentifier = schema
        ? buildFullyQualifiedObjectName({
            databaseName: database,
            schemaName: schema,
            name: "known_flippers",
          })
        : sql.id("known_flippers");
      const compiledQuery =
        sql`INSERT INTO ${qualifiedIdentifier} VALUES(${accountId.toString()})`.compile(
          queryBuilder,
        );
      return {
        queries: [
          {
            query: compiledQuery.sql,
            params: compiledQuery.parameters as string[],
          },
        ],
        cluster: cluster ?? "mz_catalog_server",
      };
    },
  });

  const flagAsflipper = (flipperAccountId: string) => {
    insertflipperAccount(parseInt(flipperAccountId), {
      onSuccess: () => {
        setFormError(null);
        reset();
      },
      onError: (errorMessage) => {
        setFormError(
          errorMessage ?? "There was an error flagging this account",
        );
      },
    });
  };

  const { formState, register, control, handleSubmit, reset } = useForm<{
    accountId: string;
  }>({
    defaultValues: {
      accountId: "",
    },
    mode: "onChange",
  });

  const formAccountId = useWatch({ name: "accountId", control });
  const flipperCommand = `INSERT INTO known_flippers VALUES(${
    !isNaN(parseInt(formAccountId)) ? parseInt(formAccountId) : "<num>"
  });`;

  const accountIdIsValid = !!(
    formState.isValid &&
    formAccountId !== "" &&
    !isNaN(parseInt(formAccountId))
  );

  return (
    <form
      onSubmit={handleSubmit((formdata) => flagAsflipper(formdata.accountId))}
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
          Enter a flipper id
        </Text>
        <FormControl isInvalid={accountIdIsValid || !!formError}>
          <Input
            {...register("accountId", {
              required: "Object name is required.",
            })}
            type="number"
            variant="default"
            size="md"
            width="100%"
            placeholder="Enter a flipper ID (e.g., 450)"
            data-testid="account-id-input"
          />
          {formError && <FormErrorMessage>{formError}</FormErrorMessage>}
        </FormControl>
        <HStack
          width="100%"
          justify="space-between"
          data-testid="flipper-command-line"
        >
          {formError}
          <ReadOnlyCommandBlock
            value={flipperCommand}
            containerProps={{ overflow: "auto" }}
          />
          <Button
            width="fit-content"
            variant="tertiary"
            paddingX={4}
            size="md"
            isDisabled={loading || !accountIdIsValid}
            type="submit"
            data-testid="account-id-submit"
            flexShrink="0"
          >
            Flag as flipper
          </Button>
        </HStack>
      </VStack>
    </form>
  );
};

export default TutorialInsertionWidget;
