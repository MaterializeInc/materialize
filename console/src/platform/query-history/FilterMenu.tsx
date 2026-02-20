// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Accordion,
  AccordionButton,
  AccordionIcon,
  AccordionItem,
  AccordionPanel,
  Button,
  Checkbox,
  Circle,
  FormControl,
  FormErrorMessage,
  HStack,
  Input,
  Popover,
  PopoverContent,
  PopoverTrigger,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { useState } from "react";
import {
  FieldError,
  FieldErrors,
  SubmitHandler,
  useController,
  useFormContext,
} from "react-hook-form";

import {
  DEFAULT_SCHEMA_VALUES,
  FINISHED_STATUSES,
  QueryHistoryListSchema,
  STATEMENT_TYPES,
} from "~/api/materialize/query-history/queryHistoryList";
import CountSignifier from "~/components/CountSignifier";
import { DropdownToggleButton } from "~/components/Dropdown/dropdownComponents";
import StatusPill from "~/components/StatusPill";
import ChevronDownIcon from "~/svg/ChevronDownIcon";
import SliderFilterIcon from "~/svg/SliderFilterIcon";
import { MaterializeTheme } from "~/theme";
import { viewportOverflowModifier } from "~/theme/components/Popover";

import {
  createMultiSelectReactHookFormHandler,
  transformEmptyStringToNull,
} from "./queryHistoryUtils";
import { calculateDirtyState, FINISHED_STATUS_TO_COLOR_SCHEME } from "./utils";

const FILTER_MENU_FORM_FIELDS = [
  "statementTypes",
  "finishedStatuses",
  "durationRange",
  "sessionId",
  "applicationName",
  "sqlText",
  "executionId",
  "showConsoleIntrospection",
] as const;

function findChangedAccordionItems(
  accordionItems1: number[],
  accordionItems2: number[],
) {
  let smallestAccordionItems: number[], largestAccordionItems: number[];

  if (accordionItems1.length < accordionItems2.length) {
    smallestAccordionItems = accordionItems1;
    largestAccordionItems = accordionItems2;
  } else {
    smallestAccordionItems = accordionItems2;
    largestAccordionItems = accordionItems1;
  }

  return largestAccordionItems.filter(
    (item) => !smallestAccordionItems.includes(item),
  );
}

const FilterMenuAccordionItem = ({
  label,
  panelContent,
  isDirty = false,
}: {
  label: string;
  panelContent?: React.ReactNode;
  isDirty?: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <AccordionItem>
      <AccordionButton>
        <Text flex="1" textAlign="left" userSelect="none">
          {label}
        </Text>
        {isDirty && (
          <Circle
            backgroundColor={colors.accent.brightPurple}
            size="2"
            mr="2"
          />
        )}
        <ChevronDownIcon
          as={AccordionIcon}
          color={colors.foreground.secondary}
        />
      </AccordionButton>
      <AccordionPanel p="0">{panelContent}</AccordionPanel>
    </AccordionItem>
  );
};

const DurationRangeErrorMessages = ({
  errors,
}: {
  errors: FieldErrors<QueryHistoryListSchema>;
}) => {
  const durationRangeError = errors.durationRange;

  if (!durationRangeError) {
    return null;
  }

  if (Object.keys(durationRangeError).length > 0) {
    const errorFields = Object.values(durationRangeError) as FieldError[];
    return (
      <>
        {errorFields.map((error, index) => {
          return (
            <FormErrorMessage key={index} mt="0">
              {error?.message}
            </FormErrorMessage>
          );
        })}
      </>
    );
  }

  return (
    <FormErrorMessage mt="0">{durationRangeError.message}</FormErrorMessage>
  );
};

type FilterMenuProps = {
  onSubmit: SubmitHandler<QueryHistoryListSchema>;
};

export const FilterMenu = (props: FilterMenuProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { register, formState, resetField, handleSubmit } =
    useFormContext<QueryHistoryListSchema>();

  const [accordionState, setAccordionState] = useState<number[]>([]);
  const { field: finishedStatusesField } = useController<
    QueryHistoryListSchema,
    "finishedStatuses"
  >({
    name: "finishedStatuses",
  });
  const { field: statementTypesField } = useController<
    QueryHistoryListSchema,
    "statementTypes"
  >({
    name: "statementTypes",
  });
  const { field: showConsoleIntrospectionField } = useController<
    QueryHistoryListSchema,
    "showConsoleIntrospection"
  >({
    name: "showConsoleIntrospection",
  });

  const { numDirtyFields, isDirtyByField } = calculateDirtyState(
    formState.dirtyFields,
  );

  const onAccordionChange = (newAccordionState: number[]) => {
    const changedAccordionItems = findChangedAccordionItems(
      accordionState,
      newAccordionState,
    );

    const isErrorInAccordionItem = FILTER_MENU_FORM_FIELDS.some(
      (field, index) =>
        formState.errors[field] && changedAccordionItems.includes(index),
    );

    if (isErrorInAccordionItem) {
      return;
    }

    setAccordionState(newAccordionState);
  };

  const onStatementTypesChange = createMultiSelectReactHookFormHandler({
    currentSelectValues: statementTypesField.value,
    onChange: statementTypesField.onChange,
  });

  const onFinishedStatusesChange = createMultiSelectReactHookFormHandler({
    currentSelectValues: finishedStatusesField.value,
    onChange: finishedStatusesField.onChange,
  });

  const openAccordionItems = FILTER_MENU_FORM_FIELDS.reduce(
    (accum, fieldKey, index) => {
      const isErrorInAccordionItem = !!formState.errors[fieldKey];

      if (isErrorInAccordionItem || accordionState.includes(index)) {
        accum.push(index);
      }
      return accum;
    },
    [] as number[],
  );

  const isErrorInAnyFilterMenuFields = FILTER_MENU_FORM_FIELDS.some(
    (fieldKey) => !!formState.errors[fieldKey],
  );

  return (
    <Popover
      gutter={1}
      modifiers={viewportOverflowModifier}
      variant="dropdown"
      placement="bottom-start"
    >
      {({ isOpen, onClose }) => {
        const onSubmit = handleSubmit(async (filters) => {
          await props.onSubmit(filters);
          onClose();
        });

        return (
          <>
            <PopoverTrigger>
              <DropdownToggleButton
                inputProps={{
                  "aria-label": "Filter menu",
                  isInvalid: isErrorInAnyFilterMenuFields,
                  variant: isOpen ? "focused" : "default",
                  boxShadow: "none",
                }}
                leftIcon={<SliderFilterIcon />}
              >
                Filter
                {numDirtyFields > 0 && (
                  <CountSignifier>{numDirtyFields}</CountSignifier>
                )}
              </DropdownToggleButton>
            </PopoverTrigger>

            <PopoverContent
              motionProps={{
                animate: false,
              }}
            >
              <VStack width="256px" alignItems="flex-start" tabIndex={-1}>
                <Accordion
                  allowMultiple
                  width="100%"
                  reduceMotion
                  index={openAccordionItems}
                  onChange={(e) => onAccordionChange(e as number[])}
                >
                  <Text
                    textStyle="text-ui-med"
                    color={colors.foreground.secondary}
                    px="4"
                    pt="3"
                    pb="2"
                  >
                    Filters
                  </Text>
                  <FilterMenuAccordionItem
                    label="SQL text"
                    isDirty={isDirtyByField["sqlText"]}
                    panelContent={
                      <FormControl
                        as={VStack}
                        isInvalid={!!formState.errors.sqlText}
                        alignItems="flex-start"
                        px={4}
                        py={1}
                      >
                        <Input
                          type="text"
                          placeholder="Enter SQL text"
                          {...register("sqlText", {
                            setValueAs: transformEmptyStringToNull,
                          })}
                        />

                        {!!formState.errors.sqlText && (
                          <FormErrorMessage mt="0">
                            {formState.errors.sqlText.message as string}
                          </FormErrorMessage>
                        )}
                      </FormControl>
                    }
                  />
                  <FilterMenuAccordionItem
                    label="Statement type"
                    isDirty={isDirtyByField["statementTypes"]}
                    panelContent={
                      <VStack alignItems="flex-start" gap={0}>
                        {STATEMENT_TYPES.map((statementType) => (
                          <Checkbox
                            px={4}
                            py={1}
                            key={statementType}
                            spacing="0"
                            width="100%"
                            display="flex"
                            flexDir="row-reverse"
                            justifyContent="space-between"
                            alignItems="center"
                            _hover={{ bg: colors.background.secondary }}
                            name={statementType}
                            aria-label={statementType}
                            isChecked={statementTypesField.value?.includes(
                              statementType,
                            )}
                            onChange={(e) => {
                              onStatementTypesChange({
                                value: statementType,
                                isSelected: e.target.checked,
                              });
                            }}
                          >
                            <Text textStyle="monospace">
                              {statementType.toUpperCase()}
                            </Text>
                          </Checkbox>
                        ))}
                      </VStack>
                    }
                  />
                  <FilterMenuAccordionItem
                    label="Status"
                    isDirty={isDirtyByField["finishedStatuses"]}
                    panelContent={
                      <VStack alignItems="flex-start" gap={0}>
                        {FINISHED_STATUSES.map((status) => (
                          <Checkbox
                            px={4}
                            py={1}
                            key={status}
                            spacing="0"
                            width="100%"
                            display="flex"
                            flexDir="row-reverse"
                            justifyContent="space-between"
                            alignItems="center"
                            _hover={{ bg: colors.background.secondary }}
                            aria-label={status}
                            name={status}
                            isChecked={finishedStatusesField.value?.includes(
                              status,
                            )}
                            onChange={(e) => {
                              onFinishedStatusesChange({
                                value: status,
                                isSelected: e.target.checked,
                              });
                            }}
                          >
                            <StatusPill
                              colorScheme={
                                FINISHED_STATUS_TO_COLOR_SCHEME[status]
                              }
                              status={status}
                            />
                          </Checkbox>
                        ))}
                      </VStack>
                    }
                  />
                  <FilterMenuAccordionItem
                    label="Duration (ms)"
                    isDirty={isDirtyByField["durationRange"]}
                    panelContent={
                      <FormControl
                        as={VStack}
                        isInvalid={!!formState.errors.durationRange}
                        alignItems="flex-start"
                        px={4}
                        py={1}
                      >
                        <HStack>
                          <Input
                            placeholder="10"
                            {...register("durationRange.minDuration", {
                              setValueAs: transformEmptyStringToNull,
                            })}
                          />
                          <Text
                            textStyle="text-ui-med"
                            color={colors.foreground.secondary}
                          >
                            to
                          </Text>

                          <Input
                            placeholder="1000"
                            {...register("durationRange.maxDuration", {
                              setValueAs: transformEmptyStringToNull,
                            })}
                          />
                        </HStack>

                        <DurationRangeErrorMessages errors={formState.errors} />
                      </FormControl>
                    }
                  />
                  <FilterMenuAccordionItem
                    label="Session ID"
                    isDirty={isDirtyByField["sessionId"]}
                    panelContent={
                      <FormControl
                        as={VStack}
                        isInvalid={!!formState.errors.sessionId}
                        alignItems="flex-start"
                        px={4}
                        py={1}
                      >
                        <Input
                          type="text"
                          placeholder="Enter session ID"
                          {...register("sessionId", {
                            setValueAs: transformEmptyStringToNull,
                          })}
                        />
                        {!!formState.errors.sessionId && (
                          <FormErrorMessage mt="0">
                            {formState.errors.sessionId.message as string}
                          </FormErrorMessage>
                        )}
                      </FormControl>
                    }
                  />
                  <FilterMenuAccordionItem
                    label="Application name"
                    isDirty={isDirtyByField["applicationName"]}
                    panelContent={
                      <FormControl
                        as={VStack}
                        isInvalid={!!formState.errors.applicationName}
                        alignItems="flex-start"
                        px={4}
                        py={1}
                      >
                        <Input
                          type="text"
                          placeholder="Enter application name"
                          {...register("applicationName", {
                            setValueAs: transformEmptyStringToNull,
                          })}
                        />

                        {!!formState.errors.applicationName && (
                          <FormErrorMessage mt="0">
                            {formState.errors.applicationName.message as string}
                          </FormErrorMessage>
                        )}
                      </FormControl>
                    }
                  />
                  <FilterMenuAccordionItem
                    label="Query ID"
                    isDirty={isDirtyByField["executionId"]}
                    panelContent={
                      <FormControl
                        as={VStack}
                        isInvalid={!!formState.errors.executionId}
                        alignItems="flex-start"
                        px={4}
                        py={1}
                      >
                        <Input
                          type="text"
                          placeholder="Enter query ID"
                          {...register("executionId", {
                            setValueAs: transformEmptyStringToNull,
                          })}
                        />

                        {!!formState.errors.executionId && (
                          <FormErrorMessage mt="0">
                            {formState.errors.executionId.message as string}
                          </FormErrorMessage>
                        )}
                      </FormControl>
                    }
                  />
                  <HStack py="1" px="4">
                    <Checkbox
                      spacing="0"
                      width="100%"
                      display="flex"
                      flexDir="row-reverse"
                      justifyContent="space-between"
                      alignItems="center"
                      aria-label="Show Console introspection"
                      isChecked={showConsoleIntrospectionField.value}
                      onChange={(e) => {
                        showConsoleIntrospectionField.onChange(
                          e.target.checked,
                        );
                      }}
                    >
                      <Text textStyle="text-ui-med">
                        Show Console introspection
                      </Text>
                    </Checkbox>
                  </HStack>
                </Accordion>
                <HStack
                  borderTopWidth="1px"
                  borderColor={colors.border.secondary}
                  width="100%"
                  justifyContent="space-between"
                  py="2"
                  px="4"
                >
                  <Button
                    variant="secondary"
                    size="sm"
                    transition="none"
                    onClick={() => {
                      resetField("statementTypes", {
                        defaultValue: DEFAULT_SCHEMA_VALUES.statementTypes,
                      });
                      resetField("finishedStatuses", {
                        defaultValue: DEFAULT_SCHEMA_VALUES.finishedStatuses,
                      });
                      resetField("showConsoleIntrospection", {
                        defaultValue:
                          DEFAULT_SCHEMA_VALUES.showConsoleIntrospection,
                      });

                      resetField("sessionId", {
                        defaultValue: DEFAULT_SCHEMA_VALUES.sessionId,
                      });
                      resetField("applicationName", {
                        defaultValue: DEFAULT_SCHEMA_VALUES.applicationName,
                      });
                      resetField("sqlText", {
                        defaultValue: DEFAULT_SCHEMA_VALUES.sqlText,
                      });
                      resetField("executionId", {
                        defaultValue: DEFAULT_SCHEMA_VALUES.executionId,
                      });

                      resetField("durationRange.minDuration", {
                        defaultValue: null,
                      });
                      resetField("durationRange.maxDuration", {
                        defaultValue: null,
                      });
                      onSubmit();
                    }}
                  >
                    Reset filters
                  </Button>
                  <Button
                    variant="primary"
                    transition="none"
                    size="sm"
                    onClick={onSubmit}
                  >
                    Apply filters
                  </Button>
                </HStack>
              </VStack>
            </PopoverContent>
          </>
        );
      }}
    </Popover>
  );
};

export default FilterMenu;
