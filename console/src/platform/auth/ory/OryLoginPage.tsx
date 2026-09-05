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
  Divider,
  FormControl,
  HStack,
  Input,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useQuery } from "@tanstack/react-query";
import React, { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";

import Alert from "~/components/Alert";
import { LabeledInput } from "~/components/formComponentsV2";
import { LoadingContainer } from "~/components/LoadingContainer";
import { MaterializeLogo } from "~/components/MaterializeLogo";
import {
  type LoginFlow,
  ResponseError,
  type UiNode,
} from "~/external-library-wrappers/ory";
import { LockIcon } from "~/icons";
import { AuthContentContainer, AuthLayout } from "~/layouts/AuthLayout";
import { MaterializeTheme } from "~/theme";

import { getOryClient } from "./oryConfig";

/**
 * Resolves the login flow for this page load.
 *
 * A `flow` in the URL means Ory redirected back to us and the flow already
 * exists. A `login_challenge` means Hydra sent the user here to authenticate
 * for an OAuth2 client, and forwarding it is what lets Kratos accept that
 * challenge on our behalf.
 */
async function fetchLoginFlow({
  flowId,
  loginChallenge,
  returnTo,
}: {
  flowId: string | null;
  loginChallenge: string | null;
  returnTo: string | null;
}): Promise<LoginFlow> {
  const api = await getOryClient();

  if (flowId) {
    try {
      return await api.getLoginFlow({ id: flowId });
    } catch (error) {
      // A flow that has expired, been consumed, or never existed is recoverable
      // by starting a fresh one. Anything else is a real failure.
      if (
        !(error instanceof ResponseError) ||
        ![404, 410].includes(error.response.status)
      ) {
        throw error;
      }
    }
  }

  const params = {
    ...(returnTo ? { returnTo } : {}),
    ...(loginChallenge ? { loginChallenge } : {}),
  };

  try {
    return await api.createBrowserLoginFlow(params);
  } catch (error) {
    // Kratos refuses to start a login for someone already signed in and asks
    // for `refresh` instead, which re-authenticates the current session rather
    // than replacing it. A returning user hitting this page wants that, not an
    // error telling them they are logged in.
    if ((await oryErrorId(error)) !== "session_already_available") throw error;
    return api.createBrowserLoginFlow({ ...params, refresh: true });
  }
}

/** The `error.id` Ory reported, or undefined when the failure isn't Ory's. */
async function oryErrorId(error: unknown): Promise<string | undefined> {
  if (!(error instanceof ResponseError)) return undefined;
  const body: { error?: { id?: string } } = await error.response
    .clone()
    .json()
    .catch(() => ({}));
  return body.error?.id;
}

/**
 * Ory states why a request failed in the response body. The thrown error itself
 * carries only "Response returned an error code", so unwrap it or the page
 * shows nothing a reader can act on.
 */
async function unwrapOryError(error: unknown): Promise<Error> {
  if (!(error instanceof ResponseError)) {
    return error instanceof Error
      ? error
      : new Error("Failed to start sign-in. Please try refreshing the page.");
  }
  const body: { error?: { message?: string; reason?: string } } =
    await error.response
      .clone()
      .json()
      .catch(() => ({}));
  // `reason` and `message` are Ory's own prose. Its `id` is a machine code and
  // the status an implementation detail, so neither is shown.
  const { message, reason } = body.error ?? {};
  return new Error(
    reason ?? message ?? "Sign-in could not be started. Please try again.",
  );
}

/** Input types the form draws. Anything else is reported, not skipped. */
const RENDERABLE_FIELD_TYPES = ["text", "email", "password", "tel"];

interface LoginFormNodes {
  /** Flow state, most importantly the CSRF token, replayed on submit. */
  hidden: { name: string; value: string }[];
  /**
   * Values the user types. Present on an identifier-first flow, which asks for
   * an email before it will say which providers can serve it.
   */
  fields: {
    name: string;
    type: string;
    label: string;
    value: string;
    required: boolean;
    autocomplete?: string;
  }[];
  /** This step's own submit, such as Continue on an identifier-first flow. */
  submits: { name: string; value: string; label: string }[];
  /** One per configured identity provider. */
  providers: { name: string; value: string; label: string }[];
  /** Set when the flow asks for something this screen cannot draw. */
  hasUnsupportedNode: boolean;
}

/**
 * Kratos describes a flow as a list of UI nodes rather than a fixed form, so
 * what a deployment configures is what a screen has to render. Anything this
 * form cannot draw is reported rather than silently dropped, since a half-drawn
 * form looks like a broken page instead of a misconfiguration.
 */
function collectNodes(nodes: UiNode[]): LoginFormNodes {
  const hidden: LoginFormNodes["hidden"] = [];
  const fields: LoginFormNodes["fields"] = [];
  const submits: LoginFormNodes["submits"] = [];
  const providers: LoginFormNodes["providers"] = [];
  let hasUnsupportedNode = false;

  for (const { attributes, group, meta } of nodes) {
    if (attributes.node_type !== "input") {
      // Scripts, text and links decorate a flow rather than carry it, and a
      // method that genuinely needs one also contributes an input this loop
      // rejects on its own. An image is different: a flow that shows one, a
      // TOTP QR code say, cannot be completed without it.
      if (attributes.node_type === "img") hasUnsupportedNode = true;
      continue;
    }
    const value = String(attributes.value ?? "");
    if (attributes.type === "hidden") {
      hidden.push({ name: attributes.name, value });
    } else if (attributes.type === "submit") {
      // A federated button hands the user to another service; the step's own
      // submit advances this form. Different actions, so they are ranked
      // differently rather than stacked as equals.
      const button = {
        name: attributes.name,
        value,
        label: meta.label?.text ?? "Sign in",
      };
      (group === "oidc" || group === "saml" ? providers : submits).push(button);
    } else if (RENDERABLE_FIELD_TYPES.includes(attributes.type)) {
      fields.push({
        name: attributes.name,
        type: attributes.type,
        // Kratos labels the identifier "ID", since it does not know whether a
        // deployment identifies people by email, username or phone.
        label: meta.label?.text ?? attributes.name,
        value,
        required: attributes.required ?? false,
        autocomplete: attributes.autocomplete,
      });
    } else {
      hasUnsupportedNode = true;
    }
  }

  return { hidden, fields, submits, providers, hasUnsupportedNode };
}

const LoginFlowForm = ({ flow }: { flow: LoginFlow }) => {
  const { hidden, fields, submits, providers, hasUnsupportedNode } =
    collectNodes(flow.ui.nodes);
  const { colors } = useTheme<MaterializeTheme>();
  const providerRef = useRef<HTMLButtonElement>(null);
  // Which button is mid-submit, so the one that was pressed shows it. Recorded
  // on the form rather than in each handler, which puts the automatic submit
  // below on the same path as a real press.
  const [submittingValue, setSubmittingValue] = useState<string>();

  // Kratos brokers any number of providers, so it always returns a chooser.
  // Where a deployment configures exactly one, that chooser is a button the
  // user has no decision to make about, and clicking through it is friction an
  // ordinary SSO sign-in does not have.
  //
  // Held back whenever the flow reports an error: a rejected attempt returns as
  // an error message on a fresh flow, and submitting that unattended would
  // bounce between here and the provider indefinitely.
  //
  // `refresh` does not hold it back. On a login flow that only means an
  // existing session is being re-authenticated, and reaching this page is
  // already the request to do that. Pressing the button proves nothing either
  // way: the provider is what verifies the person, and it still applies its own
  // credential and MFA policy when the browser arrives.
  const hasError = flow.ui.messages?.some((m) => m.type === "error") ?? false;
  const autoSubmit =
    providers.length === 1 &&
    submits.length === 0 &&
    fields.length === 0 &&
    !hasUnsupportedNode &&
    !hasError;

  // Runs once: the form is keyed on the flow, so `autoSubmit` cannot change
  // under a mounted instance. Pressing the button rather than submitting the
  // form directly is what makes the browser include its name and value, which
  // is how the flow says which provider was chosen.
  useEffect(() => {
    if (autoSubmit) providerRef.current?.click();
  }, [autoSubmit]);

  return (
    // Submitting navigates rather than fetches: Kratos answers with a redirect
    // to the identity provider, which only a top-level navigation can follow.
    <form
      action={flow.ui.action}
      method={flow.ui.method}
      onSubmit={(event) =>
        setSubmittingValue(
          (event.nativeEvent as SubmitEvent).submitter?.getAttribute("value") ??
            undefined,
        )
      }
    >
      {hidden.map((input) => (
        <input
          key={input.name}
          type="hidden"
          name={input.name}
          value={input.value}
          readOnly
        />
      ))}
      <VStack spacing="6" alignItems="stretch">
        {flow.ui.messages?.map((message, index) => (
          <Alert
            key={`${message.id}:${index}`}
            variant={message.type === "error" ? "error" : "info"}
            minWidth="100%"
            message={message.text}
          />
        ))}
        {(hasUnsupportedNode || submits.length + providers.length === 0) && (
          <Alert
            variant="warning"
            minWidth="100%"
            message="This environment is configured with a sign-in method this page does not support."
          />
        )}
        {fields.map((field) => (
          // FormControl is what gives FormLabel its htmlFor, so the label is
          // only tied to the input when one wraps them.
          <FormControl key={field.name} isRequired={field.required}>
            <LabeledInput label={field.label} variant="stretch">
              <Input
                name={field.name}
                type={field.type}
                defaultValue={field.value}
                autoComplete={field.autocomplete}
                size="lg"
              />
            </LabeledInput>
          </FormControl>
        ))}
        {submits.map((submit) => (
          <Button
            key={`${submit.name}:${submit.value}`}
            variant="primary"
            size="lg"
            width="100%"
            type="submit"
            name={submit.name}
            value={submit.value}
            isLoading={submittingValue === submit.value}
            loadingText={submit.label}
          >
            {submit.label}
          </Button>
        ))}
        {submits.length > 0 && providers.length > 0 && (
          <HStack spacing="4" aria-hidden="true">
            <Divider borderColor={colors.border.primary} />
            <Text fontSize="sm" color={colors.foreground.secondary}>
              or
            </Text>
            <Divider borderColor={colors.border.primary} />
          </HStack>
        )}
        {providers.map((provider, index) => (
          <Button
            key={`${provider.name}:${provider.value}`}
            ref={index === 0 ? providerRef : undefined}
            variant={submits.length > 0 ? "secondary" : "primary"}
            size="lg"
            width="100%"
            type="submit"
            name={provider.name}
            value={provider.value}
            isLoading={submittingValue === provider.value}
            loadingText={provider.label}
            // One mark for every provider: an approximated brand logo is worse
            // than none, and a deployment is free to name its provider
            // anything, so most would have no logo to show.
            leftIcon={<LockIcon />}
            // LockIcon bakes in a stroke colour and a 24px box. Scoped here so
            // the glyph tracks the label in both themes without changing the
            // icon for every other screen that uses it.
            sx={{
              "& svg": { width: "4", height: "4" },
              "& svg [stroke]": { stroke: "currentColor" },
            }}
          >
            {provider.label}
          </Button>
        ))}
      </VStack>
    </form>
  );
};

export const OryLoginPage = () => {
  const [searchParams] = useSearchParams();
  const flowId = searchParams.get("flow");
  const loginChallenge = searchParams.get("login_challenge");
  const returnTo = searchParams.get("return_to");

  const {
    data: flow,
    isLoading,
    error,
  } = useQuery({
    queryKey: ["oryLoginFlow", flowId, loginChallenge, returnTo],
    queryFn: async () => {
      try {
        return await fetchLoginFlow({ flowId, loginChallenge, returnTo });
      } catch (flowError) {
        throw await unwrapOryError(flowError);
      }
    },
    retry: false,
    refetchOnWindowFocus: false,
  });

  return (
    <AuthLayout>
      <AuthContentContainer>
        <VStack alignItems="stretch" width="100%" mx="12">
          <HStack my={{ base: "8", lg: "0" }} paddingBottom="8">
            <MaterializeLogo height="12" />
          </HStack>
          {error && (
            <Alert
              variant="error"
              minWidth="100%"
              message={error.message}
              mb="4"
            />
          )}
          {isLoading && <LoadingContainer />}
          {/* Keyed so a new flow starts with fresh submit state: the effect
              inside fires once per form, and an uncontrolled input keeps its
              first value. */}
          {flow && <LoginFlowForm key={flow.id} flow={flow} />}
        </VStack>
      </AuthContentContainer>
    </AuthLayout>
  );
};
