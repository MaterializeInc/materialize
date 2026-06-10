// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  BoxProps,
  Button,
  CircularProgress,
  CodeProps,
  FormControl,
  HStack,
  Radio,
  RadioGroup,
  Step,
  StepDescription,
  StepIcon,
  StepIndicator,
  StepNumber,
  Stepper,
  StepSeparator,
  StepStatus,
  StepTitle,
  Text,
  useSteps,
  UseStepsReturn,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { useEffect, useState } from "react";
import { useForm } from "react-hook-form";

import { BodyFormat } from "~/api/materialize/source/createWebhookSourceStatement";
import { Source } from "~/api/materialize/source/sourceList";
import useWebhookSourceEvents from "~/api/materialize/source/useWebhookSourceEvents";
import Alert from "~/components/Alert";
import ReadOnlyCommandBlock from "~/components/CommandBlock/ReadOnlyCommandBlock";
import { CopyableBox } from "~/components/copyableComponents";
import { LabeledInput } from "~/components/formComponentsV2";
import ObjectNameInput from "~/components/ObjectNameInput";
import { MaterializeTheme } from "~/theme";
import { assert } from "~/util";

export type TesterProps = {
  source: Source;
  bodyFormat: BodyFormat;
  viewDdl: string | null;
  headerColumns: { header: string; column: string }[];
  onViewUpdate: (viewName: string | null, sampleObject: object | null) => void;
  viewCreationError?: string;
};

type RenderProps = {
  step: number;
  source: Source;
  sampleObject: object | null;
  setSampleObject: (datum: object | null) => void;
  viewDdl: TesterProps["viewDdl"];
  onViewUpdate: TesterProps["onViewUpdate"];
  bodyFormat: BodyFormat;
  headerColumns: TesterProps["headerColumns"];
} & UseStepsReturn;

const Configure = ({ source, goToNext, step, activeStep }: RenderProps) => {
  const onCopy = () => {
    // Only advance the stepper if this component is currently active.
    if (activeStep === step) {
      goToNext();
    }
  };

  assert(source.webhookUrl);
  return (
    <CopyableBox
      variant="compact"
      contents={source.webhookUrl}
      aria-label="Webhook URL"
      onCopy={onCopy}
    />
  );
};

const PayloadPreview = ({
  payload,
  codeProps,
  checkForNewData,
}: {
  payload: string;
  codeProps: CodeProps;
  checkForNewData: () => void;
}) => (
  <Box>
    <VStack>
      <HStack width="100%" justifyContent="space-between">
        <Text textStyle="text-ui-med">Webhook Payload</Text>
        <Button variant="link" size="xs" onClick={checkForNewData}>
          Check for new data
        </Button>
      </HStack>
      <ReadOnlyCommandBlock
        value={payload}
        language="json"
        containerProps={{
          ...codeProps,
          width: "100%",
          maxHeight: "15lh",
          overflow: "auto",
          px: 4,
          py: 2,
        }}
        lineNumbers
      />
    </VStack>
  </Box>
);

const PROGRESS_INDICATOR_SIZE = 4;
const PROGRESS_INDICATOR_MARGIN = 2;

const Spy = ({
  source,
  skipped,
  onSkip,
  onDataReceived,
  containerProps,
  headerColumns,
}: {
  source: Source;
  skipped: boolean;
  onSkip: () => void;
  onDataReceived: (datum: object) => void;
  containerProps: BoxProps;
  headerColumns: string[];
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data, isError, error } = useWebhookSourceEvents({
    databaseName: source.databaseName,
    schemaName: source.schemaName,
    sourceName: source.name,
    clusterName: source.clusterName,
    headerColumns,
  });

  const record = data.find((datum) => datum.mzState !== null);
  useEffect(() => {
    if (record) {
      onDataReceived(record.data.body);
    }
  }, [record, onDataReceived]);
  if (isError) {
    return (
      <Alert
        variant="error"
        message={`There was an error listening for webhook data: ${error?.message}`}
        showLabel={false}
        data-testid="spy-error-alert"
      />
    );
  }
  return (
    <VStack padding={6} gap={4} {...containerProps}>
      <Box>
        <HStack alignItems="center" justifyContent="center">
          <CircularProgress
            color={colors.accent.purple}
            trackColor="transparent"
            size={PROGRESS_INDICATOR_SIZE}
            isIndeterminate
            marginRight={PROGRESS_INDICATOR_MARGIN}
          />
          <Text
            textStyle="text-ui-med"
            marginRight={PROGRESS_INDICATOR_MARGIN + PROGRESS_INDICATOR_SIZE}
          >
            Listening for webhook data
          </Text>
        </HStack>
        <Text textStyle="text-base" color={colors.foreground.secondary}>
          Once you configure the webhook destination, you&apos;ll begin to see
          data here.
        </Text>
      </Box>
      <Button
        data-testid="spy-skip-button"
        variant="secondary"
        onClick={onSkip}
        isDisabled={skipped}
      >
        {skipped ? "Skipped" : "Skip this step"}
      </Button>
    </VStack>
  );
};

function formatJson(datum: object): string {
  return JSON.stringify(datum, null, 2);
}

function formatBytes(byteArray: number[]): string {
  const bytes = byteArray.length > 10 ? byteArray.slice(0, 10) : byteArray;
  const stringified = formatJson(bytes);
  if (bytes.length < byteArray.length) {
    const segments = stringified.split("\n");
    segments[segments.length - 2] += ",";
    segments.splice(
      segments.length - 1,
      0,
      `  // ...${byteArray.length - bytes.length} more bytes.`,
    );
    return `// This preview has been truncated \n${segments.join("\n")}`;
  }
  return stringified;
}

const Listen = ({
  source,
  goToNext,
  goToPrevious,
  setSampleObject,
  bodyFormat,
  headerColumns,
}: RenderProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [skipped, setSkipped] = useState(false);
  const [payload, setPayload] = useState<string | null>(null);

  const columns = headerColumns.map((c) => c.column);

  const checkForNewData = () => {
    goToPrevious();
    setPayload(null);
  };

  const boxBorder = {
    border: "1px solid",
    borderColor: colors.background.secondary,
    borderRadius: "lg",
  };

  const onDataReceived = (datum: object) => {
    if (payload === null) {
      let body = datum.toString();
      if (bodyFormat.startsWith("json")) {
        body = formatJson(datum);
      } else if (bodyFormat === "bytes") {
        if (Array.isArray(datum)) {
          body = formatBytes(datum);
        } else {
          body = formatJson(datum);
        }
      }
      setPayload(body);
      setSampleObject(datum);
      goToNext();
    }
  };

  const onSkipped = () => {
    setSkipped(true);
    setSampleObject(null);
    goToNext();
  };

  return (
    <>
      {payload !== null ? (
        <PayloadPreview
          payload={payload}
          codeProps={{
            ...boxBorder,
            sx: {
              ".cm-gutter": { backgroundColor: colors.background.primary },
            },
          }}
          checkForNewData={checkForNewData}
        />
      ) : (
        <Spy
          source={source}
          containerProps={boxBorder}
          onSkip={onSkipped}
          onDataReceived={onDataReceived}
          skipped={skipped}
          headerColumns={columns}
        />
      )}
    </>
  );
};

export const Parse = ({
  source,
  sampleObject,
  viewDdl,
  onViewUpdate,
  goToNext,
}: RenderProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const boxBorder = {
    border: "1px solid",
    borderColor: colors.background.secondary,
    borderRadius: "lg",
  };
  const { formState, getValues, register, watch } = useForm<{
    shouldParse: string;
    viewName: string;
  }>({
    defaultValues: {
      shouldParse: "yes",
      viewName: `${source.name}_view`,
    },
    mode: "onTouched",
  });
  const watchShouldParse = watch("shouldParse");
  const watchViewName = watch("viewName");

  useEffect(() => {
    if (
      watchShouldParse === "yes" &&
      watchViewName !== "" &&
      formState.isValid &&
      sampleObject !== null
    ) {
      onViewUpdate(watchViewName, sampleObject);
    } else {
      onViewUpdate(null, sampleObject);
    }
  }, [
    onViewUpdate,
    watchShouldParse,
    watchViewName,
    formState.isValid,
    sampleObject,
  ]);

  useEffect(() => {
    if (sampleObject === null) {
      goToNext();
    }
  }, [sampleObject, goToNext]);

  if (sampleObject === null) {
    return (
      <Alert
        variant="info"
        message="View DDL cannot be generated without a test event from the previous step."
        showLabel={false}
        data-testid="sample-object-missing-alert"
      />
    );
  }

  return (
    <VStack>
      <FormControl isInvalid={!!formState.errors.shouldParse}>
        <RadioGroup
          data-testid="should-parse"
          defaultValue={getValues("shouldParse")}
        >
          <VStack alignItems="flex-start">
            <Radio {...register("shouldParse")} variant="form" value="yes">
              Yes, parse JSON response in a new view
            </Radio>
            <Radio {...register("shouldParse")} variant="form" value="no">
              No, I&apos;ll configure the parsing myself
            </Radio>
          </VStack>
        </RadioGroup>
      </FormControl>
      {watchShouldParse === "yes" && (
        <FormControl isInvalid={!!formState.errors.viewName}>
          <LabeledInput label="View name" inputBoxProps={{ maxWidth: "unset" }}>
            <ObjectNameInput
              {...register("viewName")}
              placeholder="view_name"
              autoCorrect="off"
              spellCheck="false"
              size="sm"
              variant={formState.errors.viewName ? "error" : "default"}
              autoFocus
            />
          </LabeledInput>
        </FormControl>
      )}
      {watchShouldParse === "yes" &&
        watchViewName !== "" &&
        formState.isValid &&
        viewDdl !== null && (
          <ReadOnlyCommandBlock
            containerProps={{
              ...boxBorder,
              width: "100%",
              px: 4,
              py: 2,
              mt: 2,
              sx: {
                ".cm-gutter": {
                  backgroundColor: colors.background.primary,
                },
              },
            }}
            value={viewDdl}
            lineNumbers
          />
        )}
    </VStack>
  );
};

const TESTER_STEPS = [
  {
    title: "Configure the webhook in your external application",
    description:
      "Copy the webhook URL below and configure the webhook destination in Segment to start streaming data into Materialize.",
    render: Configure,
  },
  {
    title: "Waiting for events",
    description:
      "Send a few test events to ensure the source is working properly.",
    render: Listen,
  },
];

const STEP_GAP = 10;
// subtract width of stepper and then flex gap
const STEP_CONTENT_WIDTH = `calc(100% - 32px - ${STEP_GAP * 4}px)`;

const Tester = ({
  source,
  bodyFormat,
  headerColumns,
  viewDdl,
  onViewUpdate,
  viewCreationError,
}: TesterProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [sampleObject, setSampleObject] = useState<object | null>(null);
  const steps = [
    ...TESTER_STEPS,
    ...(bodyFormat.startsWith("json")
      ? [
          {
            title: "Parse JSON payload",
            description:
              "Your source returns a JSON object. Would you like Materialize to parse it via a new SQL view?",
            render: Parse,
          },
        ]
      : []),
  ];
  const stepper = useSteps({ count: steps.length });
  return (
    <>
      <Stepper
        index={stepper.activeStep}
        orientation="vertical"
        gap={2}
        sx={{
          ".chakra-step__separator[data-orientation=vertical]": {
            background: colors.border.primary,
            top: "calc(var(--stepper-indicator-size) + 8px)",
          },
          ".chakra-step": {
            gap: STEP_GAP,
          },
        }}
        data-testid="tester-stepper"
      >
        {steps.map(({ title, description, render: Render }, index) => (
          <Box key={index} width="100%">
            <Step data-testid={`tester-step-${index}`}>
              <StepIndicator
                sx={{
                  "[data-status=complete] &": {
                    background: colors.accent.green,
                  },
                  "[data-status=active] &": {
                    background: colors.background.inverse,
                    color: colors.foreground.inverse,
                    borderColor: colors.border.primary,
                  },
                }}
              >
                <StepStatus
                  active={<StepNumber />}
                  complete={<StepIcon />}
                  incomplete={<StepNumber />}
                />
              </StepIndicator>
              <Box maxWidth={STEP_CONTENT_WIDTH} width="100%">
                <StepTitle>
                  <Text textStyle="heading-xs">{title}</Text>
                </StepTitle>
                {index <= stepper.activeStep && description && (
                  <Box mt={2}>
                    <StepDescription>{description}</StepDescription>
                  </Box>
                )}
                {index <= stepper.activeStep && Render && (
                  <Box mt={4} mb={16}>
                    <Render
                      source={source}
                      sampleObject={sampleObject}
                      setSampleObject={setSampleObject}
                      step={index}
                      viewDdl={viewDdl}
                      onViewUpdate={onViewUpdate}
                      bodyFormat={bodyFormat}
                      headerColumns={headerColumns}
                      {...stepper}
                    />
                  </Box>
                )}
              </Box>
              <StepSeparator />
            </Step>
          </Box>
        ))}
      </Stepper>
      {viewCreationError && (
        <Alert
          variant="error"
          message={`Could not create the view: ${viewCreationError}.`}
          showLabel={false}
          data-testid="spy-error-alert"
          maxWidth={STEP_CONTENT_WIDTH}
          width="100%"
          marginLeft="auto"
        />
      )}
    </>
  );
};

export default Tester;
