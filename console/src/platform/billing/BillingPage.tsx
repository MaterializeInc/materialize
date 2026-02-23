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
  ButtonProps,
  chakra,
  Checkbox,
  Grid,
  GridItem,
  HStack,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalHeader,
  ModalOverlay,
  Spinner,
  StackProps,
  Text,
  Tooltip,
  useDisclosure,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import {
  AddressElement,
  Elements,
  LinkAuthenticationElement,
  PaymentElement,
  useElements,
  useStripe,
} from "@stripe/react-stripe-js";
import { Appearance, Stripe } from "@stripe/stripe-js";
import { useQueryClient } from "@tanstack/react-query";
import React, { useMemo, useState } from "react";

import { queryKeys as authQueryKeys, useCurrentOrganization } from "~/api/auth";
import { Organization } from "~/api/cloudGlobalApi";
import Alert, {
  AlertBanner,
  AlertBannerContent,
  AlertVariant,
} from "~/components/Alert";
import ErrorBox from "~/components/ErrorBox";
import IFrameWrapper from "~/components/IFrameWrapper";
import { Modal } from "~/components/Modal";
import { User } from "~/external-library-wrappers/frontegg";
import { useToast } from "~/hooks/useToast";
import { CalendarIcon, PaymentIcon, PlusIcon } from "~/icons";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { MaterializeTheme } from "~/theme";
import { buildStripeInputStyles } from "~/theme/components/Input";
import {
  formatDateInUtc,
  FRIENDLY_DATE_FORMAT,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import { EvaluationPlanDetails, UpgradedPlanDetails } from "./PlanDetails";
import {
  useDetachPaymentMethod,
  useInitializeSetupIntent,
  useSetDefaultPaymentMethod,
} from "./queries";
import {
  calculateNextOnDemandPaymentDate,
  getIsTrialExpired,
  getIsUpgradedPlan,
  getIsUpgrading,
} from "./utils";

const INTER_FONT_CSS_URL =
  "https://fonts.googleapis.com/css2?family=Inter:ital,opsz,wght@0,14..32,100..900;1,14..32,100..900&display=swap";

function buildStripeAppearance(theme: MaterializeTheme): Appearance {
  return {
    theme: "flat" as const,
    variables: {
      colorPrimary: theme.colors.accent.brightPurple,
      colorBackground: theme.colors.background.primary,
      colorText: theme.colors.foreground.primary,
      colorDanger: theme.colors.accent.red,
      fontFamily: "Inter",
      fontSizeBase: "14px",
      spacingUnit: "8px",
      spacingGridRow: "16px",
      borderRadius: theme.radii.lg,
    },
    rules: {
      ".Label": {
        fontSize: "14px",
        fontWeight: "500",
        marginBottom: "8px",
        color: "inherit",
      },
      ".AccordionItem": {
        backgroundColor: theme.colors.background.primary,
      },
      ...buildStripeInputStyles(theme),
    },
  };
}

function buildCardStyles(theme: MaterializeTheme) {
  return {
    borderRadius: "lg",
    boxShadow: theme.shadows.level1,
    width: "280px",
    height: "160px",
    px: "4",
  };
}

const PaymentSetupForm = ({
  onClose,
  organization,
  user,
}: {
  onClose: () => void;
  organization: Organization;
  user: User;
}) => {
  const stripe = useStripe();
  const elements = useElements();
  const { colors } = useTheme<MaterializeTheme>();

  const [acceptTerms, setAcceptTerms] = useState(false);
  const [generalFormError, setGeneralFormError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const queryClient = useQueryClient();

  const { email } = user;

  const isUpgradedPlan = getIsUpgradedPlan(organization.subscription?.type);

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();

    setIsSubmitting(true);

    try {
      if (stripe === null || elements === null) {
        throw new Error("Stripe is not initialized");
      }

      const { error } = await stripe.confirmSetup({
        elements,
        redirect: "if_required",
        confirmParams: {
          return_url: window.location.href,
        },
      });

      if (error) {
        if (error.type !== "validation_error" && error.message) {
          setGeneralFormError(error.message);
        }
      } else {
        queryClient.invalidateQueries({
          queryKey: authQueryKeys.currentOrganization(),
        });
        onClose();
      }
    } catch (e) {
      setGeneralFormError(
        e instanceof Error ? e.message : "An unexpected error occurred",
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <form onSubmit={handleSubmit}>
      <VStack spacing="8" align="stretch">
        <LinkAuthenticationElement />
        <VStack alignItems="stretch">
          <Text
            textStyle="text-small-heavy"
            color={colors.foreground.secondary}
          >
            Billing Address
          </Text>
          <AddressElement
            options={{
              mode: "billing",
            }}
          />
        </VStack>
        <VStack alignItems="stretch">
          <Text
            textStyle="text-small-heavy"
            color={colors.foreground.secondary}
          >
            Credit Card Details
          </Text>
          <PaymentElement
            options={{
              terms: {
                card: "never",
              },
              layout: "tabs",

              defaultValues: {
                billingDetails: {
                  email,
                },
              },
            }}
          />
        </VStack>

        <VStack alignItems="stretch" spacing="3">
          <Text textStyle="text-small" as="i">
            <PaymentIcon
              color={colors.foreground.primary}
              marginRight="2"
              height="4"
              width="4"
            />
            Payment will be automatically deducted from this card on the first
            of every month.
          </Text>
          <VStack alignItems="start">
            <Checkbox
              isChecked={acceptTerms}
              onChange={(e) => setAcceptTerms(e.target.checked)}
            >
              <Text textStyle="text-ui-med">Accept Terms & Conditions</Text>
            </Checkbox>

            {/* TODO (SangJunBak): Change this to terms and conditions once it's published */}
            <IFrameWrapper
              src="https://materialize.com/pdfs/on-demand-terms.pdf"
              width="100%"
              height="300px"
              title="Materialize On Demand Terms PDF"
            />
          </VStack>
        </VStack>
        {generalFormError && (
          // TODO (SangJunBak): Add a test to test the alert.
          <Alert variant="error" message={generalFormError} />
        )}
      </VStack>
      <HStack justifyContent="right" mt="5">
        <Button variant="secondary" onClick={onClose} size="sm">
          Cancel
        </Button>
        <Button
          type="submit"
          variant="primary"
          isLoading={isSubmitting}
          isDisabled={!stripe || !acceptTerms}
          size="sm"
        >
          {isUpgradedPlan ? "Add payment method" : "Upgrade & Pay"}
        </Button>
      </HStack>
    </form>
  );
};

const AddPaymentMethodButton = (props: ButtonProps) => {
  const theme = useTheme<MaterializeTheme>();
  const { colors } = theme;
  return (
    <chakra.button
      {...buildCardStyles(theme)}
      display="flex"
      alignItems="center"
      justifyContent="center"
      {...props}
    >
      <VStack>
        <PlusIcon color={colors.foreground.primary} height="6" width="6" />
        <Text textStyle="text-small-heavy">Add payment method</Text>
      </VStack>
    </chakra.button>
  );
};

const PaymentMethod = ({
  last4,
  expMonth,
  expYear,
  isDefault,
  onSetAsDefault,
  onDelete,
  isDeletable,
  containerProps,
}: {
  last4: string;
  expMonth: number;
  expYear: number;
  isDefault: boolean;
  isDeletable: boolean;

  onSetAsDefault?: () => void;
  onDelete?: () => void;

  containerProps?: StackProps;
}) => {
  const theme = useTheme<MaterializeTheme>();
  const { colors } = theme;

  const deleteButton = (
    <Button
      variant="borderless"
      size="xs"
      mr="-2"
      isDisabled={!isDeletable}
      onClick={onDelete}
    >
      Delete
    </Button>
  );

  return (
    <VStack
      alignItems="start"
      justifyContent="space-between"
      {...buildCardStyles(theme)}
      {...containerProps}
    >
      <VStack mt="5" spacing="2" alignItems="start">
        <Text textStyle="text-ui-reg">Card ending in {last4}</Text>
        <Text textStyle="text-small">
          Expires {expMonth}/{expYear}
        </Text>
      </VStack>

      <HStack justifyContent="space-between" width="100%" mb="1">
        {isDefault ? (
          <Text
            textStyle="text-small"
            color={colors.foreground.secondary}
            as="i"
          >
            Default
          </Text>
        ) : (
          <Button
            variant="borderless"
            size="xs"
            ml="-2"
            onClick={onSetAsDefault}
          >
            Set as default
          </Button>
        )}

        {isDeletable ? (
          deleteButton
        ) : (
          <Tooltip label="There must be at least one active payment method. In order to cancel a subscription, please contact support.">
            {deleteButton}
          </Tooltip>
        )}
      </HStack>
    </VStack>
  );
};

// We only support 2 payment methods in our cloud global API.
const MAX_PAYMENT_METHODS = 2;

const PlanText = ({ organization }: { organization: Organization }) => {
  const planType = organization.subscription?.type;

  switch (planType) {
    case "evaluation": {
      const trialExpirationDate = organization.trialExpiresAt
        ? new Date(organization.trialExpiresAt)
        : null;

      const hasTrialEnded = getIsTrialExpired(organization);

      return (
        <Text textStyle="text-small">
          Free trial
          {!hasTrialEnded && trialExpirationDate
            ? ` ends on ${formatDateInUtc(
                trialExpirationDate,
                FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
              )}`
            : " has ended"}
        </Text>
      );
    }

    case "on-demand": {
      const nextPaymentDate = calculateNextOnDemandPaymentDate();
      return (
        <VStack alignItems="start" spacing="0">
          <Text textStyle="text-small">
            Next payment on{" "}
            {formatDateInUtc(nextPaymentDate, FRIENDLY_DATE_FORMAT)}
          </Text>
          <Text textStyle="text-small">On-Demand plan, paid monthly</Text>
        </VStack>
      );
    }
    case "capacity":
      return <Text textStyle="text-small">Capacity plan</Text>;
    case "internal":
      return <Text textStyle="text-small">Internal plan</Text>;
    default:
      return null;
  }
};

const BillingDetails = ({
  addPaymentMethodButtonProps,
  organization,
}: {
  addPaymentMethodButtonProps?: ButtonProps;
  organization: Organization;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const toast = useToast();

  const queryClient = useQueryClient();

  const { mutate: setDefaultPaymentMethod } = useSetDefaultPaymentMethod();

  const { mutate: detachPaymentMethod } = useDetachPaymentMethod();

  const invalidateOrganizations = () => {
    queryClient.invalidateQueries({
      queryKey: authQueryKeys.currentOrganization(),
    });
  };

  const paymentMethods = organization.paymentMethods ?? [];

  return (
    <VStack alignItems="start" spacing="4">
      <Text textStyle="heading-xs">Billing Details</Text>
      <HStack spacing="2">
        <CalendarIcon
          color={colors.foreground.secondary}
          height="6"
          width="6"
        />
        <PlanText organization={organization} />
      </HStack>

      <HStack spacing="10" flexWrap="wrap">
        {paymentMethods.length > 0 &&
          paymentMethods.map((paymentMethod) => (
            <PaymentMethod
              key={paymentMethod.id}
              last4={paymentMethod.card?.last4 ?? ""}
              expMonth={paymentMethod.card?.expMonth ?? 0}
              expYear={paymentMethod.card?.expYear ?? 0}
              isDefault={paymentMethod.defaultPaymentMethod}
              isDeletable={paymentMethods.length > 1}
              onSetAsDefault={() => {
                setDefaultPaymentMethod(paymentMethod.id, {
                  onError: () => {
                    toast({
                      title: "Error",
                      description: "Failed to set default payment method",
                      status: "error",
                    });
                  },
                });
                invalidateOrganizations();
              }}
              onDelete={() => {
                detachPaymentMethod(paymentMethod.id, {
                  onError: () => {
                    toast({
                      title: "Error",
                      description: "Failed to delete payment method",
                      status: "error",
                    });
                  },
                });
                invalidateOrganizations();
              }}
            />
          ))}
        {paymentMethods.length < MAX_PAYMENT_METHODS && (
          <AddPaymentMethodButton {...addPaymentMethodButtonProps} />
        )}
      </HStack>
    </VStack>
  );
};

const OnDemandPricing = () => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <>
      <Text textStyle="heading-xs" mb="2">
        On-Demand Pricing
      </Text>
      <Text textStyle="text-small" mb="4" color={colors.foreground.secondary}>
        The following represents our on-demand pricing structure for compute,
        storage, and network resources. To calculate your estimated compute
        costs, multiply your projected compute credit usage by the listed price
        per credit.
      </Text>
      <HStack alignItems="start">
        <IFrameWrapper
          src="https://materialize.com/pdfs/pricing.pdf"
          title="Materialize Pricing PDF"
          width="100%"
          height="320px"
        />
      </HStack>
    </>
  );
};

const CTABanner = ({
  organization,
  onUpgradeAndPay,
}: {
  organization: Organization;
  onUpgradeAndPay: () => void;
}) => {
  const content = {
    title: null as React.ReactNode,
    description: null as React.ReactNode,
    variant: "success" as AlertVariant,
  };

  const { colors } = useTheme<MaterializeTheme>();

  const hasTrialEnded = getIsTrialExpired(organization);

  const upgradeAndPayButton = (
    <Button
      variant="link"
      size="xs"
      onClick={onUpgradeAndPay}
      color={colors.accent.brightPurple}
    >
      <Text textStyle="text-ui-med">Upgrade & Pay</Text>
    </Button>
  );

  const isUpgrading = getIsUpgrading(organization);

  if (isUpgrading) {
    content.title = "Upgrading";
    content.description = (
      <HStack justifyContent="space-between" width="100%">
        <Text>Your account is being upgraded to an on-demand account.</Text>
        <Spinner width="4" height="4" />
      </HStack>
    );
    content.variant = "info" as const;
  } else if (hasTrialEnded) {
    content.title = "Trial Ended";
    content.description = (
      <HStack justifyContent="space-between" width="100%">
        <Text>
          We hope you enjoyed your free trial. Upgrade to an on-demand account
          to continue using Materialize!
        </Text>
        {upgradeAndPayButton}
      </HStack>
    );
    content.variant = "error" as const;
  } else if (organization.subscription?.type === "evaluation") {
    content.title = "Trial Ending Soon";
    content.description = (
      <HStack justifyContent="space-between" width="100%">
        <Text>
          We hope you are enjoying your free trial. Upgrade to an on-demand
          account to continue using Materialize!
        </Text>
        {upgradeAndPayButton}
      </HStack>
    );
    content.variant = "warning" as const;
  } else {
    return null;
  }

  return (
    <AlertBanner variant={content.variant} justifyContent="start">
      <AlertBannerContent variant={content.variant} title={content.title}>
        {content.description}
      </AlertBannerContent>
    </AlertBanner>
  );
};

const BillingPage = ({
  user,
  stripePromise,
}: {
  user: User;
  stripePromise: Promise<Stripe | null>;
}) => {
  const { isOpen, onOpen, onClose } = useDisclosure();
  const { organization } = useCurrentOrganization({
    refetchInterval: 5000,
  });
  const toast = useToast();

  const { data: clientSecret, mutate: initializePayment } =
    useInitializeSetupIntent();

  const theme = useTheme<MaterializeTheme>();

  const stripeAppearance = useMemo(() => buildStripeAppearance(theme), [theme]);

  const handleUpgrade = () => {
    initializePayment(undefined, {
      onSuccess: () => {
        onOpen();
      },
      onError: (error) => {
        toast({
          title: "Error",
          description: "Failed to initialize payment form",
          status: "error",
        });
      },
    });
  };

  if (!organization || organization.subscription === null) {
    return <ErrorBox message="Organization does not exist or is invalid" />;
  }

  return (
    <>
      <CTABanner organization={organization} onUpgradeAndPay={handleUpgrade} />
      <MainContentContainer width="100%" maxWidth="1400px" mx="auto">
        <Grid
          templateAreas={`
            "billingDetails planDetails"
            "onDemandPricing onDemandPricing"
            `}
          gridTemplateColumns="minmax(500px, 70%) minmax(300px, 3fr)"
          gridColumnGap={12}
          gridRowGap={10}
          width="100%"
        >
          <GridItem area="billingDetails">
            <BillingDetails
              organization={organization}
              addPaymentMethodButtonProps={{ onClick: handleUpgrade }}
            />
          </GridItem>
          <GridItem area="planDetails">
            {organization?.subscription?.type === "evaluation" ? (
              <EvaluationPlanDetails
                upgradeButtonProps={{ onClick: handleUpgrade }}
              />
            ) : (
              // We don't want to display a daily average or a last 30 days summary,
              // thus we pass null for dailyCosts and timeSpan.
              <UpgradedPlanDetails
                region="all"
                dailyCosts={null}
                timeSpan={null}
              />
            )}
          </GridItem>
          <GridItem area="onDemandPricing">
            <OnDemandPricing />
          </GridItem>
        </Grid>
        <Modal isOpen={isOpen} onClose={onClose} size="xl">
          <ModalOverlay />
          <ModalContent>
            <ModalHeader>
              <Text textStyle="heading-md">Payment Information</Text>
            </ModalHeader>
            <ModalCloseButton />
            <ModalBody pb={6}>
              {clientSecret && (
                <Elements
                  stripe={stripePromise}
                  options={{
                    clientSecret,
                    appearance: stripeAppearance,
                    fonts: [
                      {
                        // We need to load the Inter font again in the iframe provided by Stripe's sdk.
                        cssSrc: INTER_FONT_CSS_URL,
                      },
                    ],
                  }}
                >
                  <PaymentSetupForm
                    onClose={onClose}
                    organization={organization}
                    user={user}
                  />
                </Elements>
              )}
            </ModalBody>
          </ModalContent>
        </Modal>
      </MainContentContainer>
    </>
  );
};

export default BillingPage;
