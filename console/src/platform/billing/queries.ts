// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo } from "react";

import { queryKeys as authQueryKeys } from "~/api/auth";
import {
  buildGlobalQueryKey,
  buildQueryKeyPart,
} from "~/api/buildQueryKeySchema";
import {
  createStripeSetupIntent,
  detachPaymentMethod,
  getCostsBreakdownDaily,
  getCredits,
  Organization,
  recentInvoices,
  setDefaultPaymentMethod,
} from "~/api/cloudGlobalApi";
import { addUtcDays, nowUTC } from "~/util";

export const creditBalanceQueryKeys = {
  all: () => buildGlobalQueryKey("credits"),
};

export const invoiceQueryKeys = {
  all: () => buildGlobalQueryKey("invoices"),
};

export const costBreakdownDailyQueryKeys = {
  all: () => buildGlobalQueryKey("costBreakdownDaily"),
  list: (timeSpan: number, queryTime: Date) =>
    [
      ...costBreakdownDailyQueryKeys.all(),
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

// `date-fns`'s `startOfDay`/`addDays` read and set calendar fields in the
// browser's local time zone, not UTC. Near UTC midnight, "local tomorrow" can
// actually be UTC "today" (or "the day after tomorrow"), silently shifting the
// whole window by a day. `addUtcDays` stays entirely within UTC calendar
// arithmetic instead.
export function getDayAlignedRange(span: number): [Date, Date] {
  const now = nowUTC();
  return [addUtcDays(now, 1 - span), addUtcDays(now, 1)];
}

/**
 * Per-account, per-cluster breakdown bucketed by UTC day (the Direction-B
 * source). Returns the dense `days` array from `/api/costs/breakdown/daily`;
 * callers pivot it via `dailyBreakdown.ts` into the roll-up / per-account views.
 */
export function useDailyCostsBreakdown(timeSpan: number, queryTime: Date) {
  return useQuery({
    queryKey: costBreakdownDailyQueryKeys.list(timeSpan, queryTime),
    queryFn: async ({ queryKey, signal }) => {
      const [_, { timeSpan: queryTimeSpan }] = queryKey;
      // Day buckets require a UTC-midnight-aligned window, which
      // getDayAlignedRange already produces.
      const [startDate, endDate] = getDayAlignedRange(queryTimeSpan);
      const response = await getCostsBreakdownDaily(startDate, endDate, {
        signal,
      });
      return response.data.days;
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
