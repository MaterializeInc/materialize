// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  queryOptions,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";

import { buildGlobalQueryKey } from "~/api/buildQueryKeySchema";
import {
  createSupportTicket,
  CreateSupportTicketRequest,
  getSupportTicket,
  listSupportTickets,
  SupportTicket,
} from "~/api/cloudGlobalApi";
import { OpenApiFetchError } from "~/api/OpenApiFetchError";
import { useAuthUser } from "~/external-library-wrappers/frontegg";

/**
 * Tickets belong to the signed-in user, and the query cache outlives a sign
 * out, so the keys include who's signed in and to which organization.
 */
export type TicketOwner = { userId: string; organizationId: string };

export const supportTicketQueryKeys = {
  all: () => buildGlobalQueryKey("support-tickets"),
  owner: (owner: TicketOwner) =>
    [...supportTicketQueryKeys.all(), owner] as const,
  list: (owner: TicketOwner) =>
    [...supportTicketQueryKeys.owner(owner), "list"] as const,
  detail: (owner: TicketOwner, ticketId: string) =>
    [...supportTicketQueryKeys.owner(owner), "detail", ticketId] as const,
};

export const useTicketOwner = (): TicketOwner => {
  const user = useAuthUser();
  return { userId: user.id, organizationId: user.tenantId };
};

/**
 * Status codes that mean the ticket endpoints can't be used, rather than a
 * transient failure: 503 when Intercom isn't configured, 404 when the global
 * API doesn't have the endpoints yet.
 */
const UNAVAILABLE_STATUSES = [404, 503];

/** Whether an error means the ticket endpoints can't be used here. */
export const isTicketsUnavailableError = (error: unknown) =>
  error instanceof OpenApiFetchError &&
  UNAVAILABLE_STATUSES.includes(error.status);

/**
 * Retries network errors and server errors that may pass, but not 503 (not
 * configured) or 4xx responses such as 401, 403, 404 and 409, which won't
 * succeed on a retry and would only hold the error back.
 */
const retryTransientErrors = (failureCount: number, error: unknown) => {
  if (failureCount >= 2) return false;
  if (!(error instanceof OpenApiFetchError)) return true;
  return error.status >= 500 && error.status !== 503;
};

/**
 * Each request is a couple of Intercom calls, and every customer shares one
 * Intercom app rate limit, so tickets aren't refetched on every window focus
 * while they're this fresh.
 */
const TICKETS_STALE_TIME_MS = 60_000;

/** The most tickets the global API returns in one page. */
const TICKETS_PAGE_SIZE = 50;

/**
 * How many pages to load at most, so a user with a very long history can't
 * run up Intercom calls. The API returns the most recently updated first.
 */
const MAX_TICKET_PAGES = 10;

export type SupportTicketList = {
  tickets: SupportTicket[];
  /** More tickets exist than were loaded (see `MAX_TICKET_PAGES`). */
  isTruncated: boolean;
};

/**
 * All of the signed-in user's tickets, so the table can filter and page
 * through them. Shared by the page and by the nav item, which prefetches it.
 *
 * The global API hides internal and unshared tickets after searching, so a
 * page can come back with fewer tickets, or none, while there are more pages.
 */
export const supportTicketsQueryOptions = (owner: TicketOwner) =>
  queryOptions({
    queryKey: supportTicketQueryKeys.list(owner),
    queryFn: async ({ signal }): Promise<SupportTicketList> => {
      const tickets: SupportTicket[] = [];
      let cursor: string | undefined;
      for (let page = 0; page < MAX_TICKET_PAGES; page++) {
        const { data } = await listSupportTickets(
          { cursor, limit: TICKETS_PAGE_SIZE },
          { signal },
        );
        tickets.push(...data.data);
        cursor = data.nextCursor ?? undefined;
        if (!cursor) return { tickets, isTruncated: false };
      }
      return { tickets, isTruncated: true };
    },
    retry: retryTransientErrors,
    staleTime: TICKETS_STALE_TIME_MS,
  });

/**
 * The signed-in user's tickets. While this errors, the page falls back to the
 * Messenger and Help Center links, so it works before the endpoints are
 * deployed.
 */
export function useSupportTickets() {
  return useQuery(supportTicketsQueryOptions(useTicketOwner()));
}

export function useSupportTicket(ticketId: string | null) {
  const owner = useTicketOwner();
  return useQuery({
    queryKey: supportTicketQueryKeys.detail(owner, ticketId ?? ""),
    queryFn: async ({ signal }) => {
      const { data } = await getSupportTicket(ticketId ?? "", { signal });
      return data;
    },
    enabled: ticketId !== null,
    retry: retryTransientErrors,
    staleTime: TICKETS_STALE_TIME_MS,
  });
}

/**
 * A ticket's row from the loaded list, so its title and state can show while
 * its replies load.
 */
export function useListedSupportTicket(
  ticketId: string | null,
): SupportTicket | undefined {
  const queryClient = useQueryClient();
  const owner = useTicketOwner();
  if (ticketId === null) return undefined;
  const list = queryClient.getQueryData(
    supportTicketsQueryOptions(owner).queryKey,
  );
  return list?.tickets.find((ticket) => ticket.id === ticketId);
}

export function useCreateSupportTicket() {
  const queryClient = useQueryClient();
  const owner = useTicketOwner();
  return useMutation({
    mutationFn: async (ticket: CreateSupportTicketRequest) => {
      const { data } = await createSupportTicket(ticket);
      return data;
    },
    onSuccess: ({ pagesOnCall: _, ...ticket }) => {
      const { queryKey } = supportTicketsQueryOptions(owner);
      // Intercom's ticket search takes a few seconds to include a new ticket,
      // so a refetch now would come back without it. Add it to the loaded
      // list instead, and refetch the next time the list is used.
      queryClient.setQueryData(queryKey, (list) =>
        !list || list.tickets.some(({ id }) => id === ticket.id)
          ? list
          : { ...list, tickets: [ticket, ...list.tickets] },
      );
      return queryClient.invalidateQueries({ queryKey, refetchType: "none" });
    },
  });
}
