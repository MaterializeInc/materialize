// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { addDays, startOfDay, subDays } from "date-fns";
import { useMemo } from "react";

import { queryKeys as authQueryKeys } from "~/api/auth";
import {
  buildGlobalQueryKey,
  buildQueryKeyPart,
} from "~/api/buildQueryKeySchema";
import {
  createStripeSetupIntent,
  detachPaymentMethod,
  getCredits,
  getDailyCosts,
  Organization,
  recentInvoices,
  setDefaultPaymentMethod,
} from "~/api/cloudGlobalApi";
import { nowUTC } from "~/util";

import { ROLLING_AVG_TIME_RANGE_LOOKBACK_DAYS } from "./constants";

export const creditBalanceQueryKeys = {
  all: () => buildGlobalQueryKey("credits"),
};

export const invoiceQueryKeys = {
  all: () => buildGlobalQueryKey("invoices"),
};

export const dailyCostQueryKeys = {
  all: () => buildGlobalQueryKey("dailyCosts"),
  list: (timeSpan: number, queryTime: Date) =>
    [
      ...dailyCostQueryKeys.all(),
      buildQueryKeyPart("list", {
        timeSpan,
        queryTime,
      }),
    ] as const,
};

async function getCreditBalance(requestOptions?: RequestInit): Promise<number> {
  const { data: responseBody } = await getCredits(requestOptions);
  return responseBody.data.reduce(
    (totalBalance, creditBlock) => totalBalance + creditBlock.balance,
    0,
  );
}

export function useCreditBalance() {
  return useQuery({
    queryKey: creditBalanceQueryKeys.all(),
    queryFn: ({ signal }) => {
      return getCreditBalance({ signal });
    },
  });
}

export function useRecentInvoices() {
  return useQuery({
    queryKey: invoiceQueryKeys.all(),
    queryFn: async ({ signal }) => {
      const { data: responseBody } = await recentInvoices({}, { signal });
      return responseBody.data;
    },
  });
}

export function getTimeRange(span: number): [Date, Date] {
  // Some fields on the usage page, such as the rolling average in the plan
  // details component, requires a certain number of days' worth of data. That
  // span of time may be greater than what is being visually filtered, so set
  // the minimum-queried range to what the rolling average needs.
  span = Math.max(span, ROLLING_AVG_TIME_RANGE_LOOKBACK_DAYS);
  const endDate = startOfDay(addDays(nowUTC(), 1));
  endDate.setUTCHours(0, 0, 0, 0);
  const startDate = new Date(
    subDays(endDate, span)
      // Since we're bucketing these by day, start from the beginning of the
      // UTC day.
      .setUTCHours(0, 0, 0, 0),
  );
  return [startDate, endDate];
}

export function useDailyCosts(timeSpan: number, queryTime: Date) {
  return useQuery({
    queryKey: dailyCostQueryKeys.list(timeSpan, queryTime),
    queryFn: async ({ queryKey, signal }) => {
      const [_, { timeSpan: queryTimeSpan }] = queryKey;
      const [startDate, endDate] = getTimeRange(queryTimeSpan);
      const response = await getDailyCosts(startDate, endDate, { signal });
      return response.data;
    },
  });
}

export type CreditBalanceResponse = ReturnType<typeof useCreditBalance>;

export function useInitializeSetupIntent() {
  return useMutation({
    mutationFn: async () => {
      const setupIntent = await createStripeSetupIntent();

      return setupIntent.data.clientSecret;
    },
  });
}

const useOrganizationOptimisticUpdate = () => {
  const queryClient = useQueryClient();

  return useMemo(
    () => ({
      startOptimisticUpdate: async () => {
        // We cancel active queries to prevent the query from being refetched.
        await queryClient.cancelQueries({
          queryKey: authQueryKeys.currentOrganization(),
        });
      },

      cancelOptimisticUpdate: (previousOrg: Organization) => {
        queryClient.setQueryData(
          authQueryKeys.currentOrganization(),
          previousOrg,
        );
      },

      getPreviousOrg: () => {
        return queryClient.getQueryData<Organization>(
          authQueryKeys.currentOrganization(),
        );
      },

      optimisticUpdate: (updater: (org: Organization) => Organization) => {
        queryClient.setQueryData(authQueryKeys.currentOrganization(), updater);
      },
    }),
    [queryClient],
  );
};

export function useSetDefaultPaymentMethod() {
  const {
    startOptimisticUpdate,
    cancelOptimisticUpdate,
    getPreviousOrg,
    optimisticUpdate,
  } = useOrganizationOptimisticUpdate();

  return useMutation({
    mutationFn: async (paymentMethodId: string) => {
      await setDefaultPaymentMethod(paymentMethodId);
    },
    onMutate: async (paymentMethodId) => {
      await startOptimisticUpdate();

      const previousOrg = getPreviousOrg();

      optimisticUpdate((old: Organization) => ({
        ...old,
        paymentMethods: old.paymentMethods?.map((pm) => ({
          ...pm,
          defaultPaymentMethod: pm.id === paymentMethodId,
        })),
        defaultPaymentMethod: paymentMethodId,
      }));

      return { previousOrg };
    },
    onError: (_err, _paymentMethodId, context) => {
      if (context?.previousOrg) {
        cancelOptimisticUpdate(context.previousOrg);
      }
    },
  });
}

export function useDetachPaymentMethod() {
  const {
    startOptimisticUpdate,
    cancelOptimisticUpdate,
    getPreviousOrg,
    optimisticUpdate,
  } = useOrganizationOptimisticUpdate();

  return useMutation({
    mutationFn: async (paymentMethodId: string) => {
      await detachPaymentMethod(paymentMethodId);
    },
    // We optimistically update the payment methods list.
    onMutate: async (paymentMethodId) => {
      await startOptimisticUpdate();

      const previousOrg = getPreviousOrg();

      optimisticUpdate((old: Organization) => ({
        ...old,
        paymentMethods: old.paymentMethods?.filter(
          (pm) => pm.id !== paymentMethodId,
        ),
      }));

      return { previousOrg };
    },
    onError: (_err, _paymentMethodId, context) => {
      if (context?.previousOrg) {
        cancelOptimisticUpdate(context.previousOrg);
      }
    },
  });
}
