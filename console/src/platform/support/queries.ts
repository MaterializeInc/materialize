// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { createStandaloneToast } from "@chakra-ui/react";
import {
  infiniteQueryOptions,
  MutationState,
  QueryClient,
  useInfiniteQuery,
  useMutation,
  useMutationState,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { millisecondsInHour, millisecondsInSecond } from "date-fns/constants";
import React from "react";

import { buildGlobalQueryKey } from "~/api/buildQueryKeySchema";
import {
  AttachedSupportTicketFiles,
  attachSupportTicketFiles,
  createSupportTicket,
  CreateSupportTicketRequest,
  getSupportTicket,
  listSupportOrganizations,
  listSupportTickets,
  SupportTicket,
} from "~/api/cloudGlobalApi";
import { OpenApiFetchError } from "~/api/OpenApiFetchError";
import { useAuthUser } from "~/external-library-wrappers/frontegg";
import { TOAST_DEFAULTS } from "~/hooks/useToast";
import { pluralize } from "~/util";

import { attachmentFileName, readAsBase64 } from "./ticketForm";

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
  organizations: (owner: TicketOwner) =>
    [...supportTicketQueryKeys.owner(owner), "organizations"] as const,
  /** Adding files to a new ticket. Intercom's ticket IDs are unique. */
  attachFiles: (ticketId: string) =>
    [...supportTicketQueryKeys.all(), "attach-files", ticketId] as const,
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

/** Keep the first load small because Intercom returns full conversations. */
const INITIAL_TICKETS_PAGE_SIZE = 10;

/** The most tickets the global API returns in later pages. */
const TICKETS_PAGE_SIZE = 50;

/**
 * How many pages to load at most, so a user with a very long history can't
 * run up Intercom calls. The API returns the most recently updated first.
 */
const MAX_TICKET_PAGES = 10;

/**
 * The signed-in user's tickets, one API page per query page. Shared by the
 * page and by the nav item, which prefetches the first page.
 *
 * The global API hides internal and unshared tickets after searching, so a
 * page can come back with fewer tickets, or none, while there are more pages.
 */
export const supportTicketsQueryOptions = (owner: TicketOwner) =>
  infiniteQueryOptions({
    queryKey: supportTicketQueryKeys.list(owner),
    queryFn: async ({ pageParam, signal }) => {
      const { data } = await listSupportTickets(
        {
          cursor: pageParam,
          limit:
            pageParam === undefined
              ? INITIAL_TICKETS_PAGE_SIZE
              : TICKETS_PAGE_SIZE,
        },
        { signal },
      );
      return data;
    },
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (lastPage, allPages) =>
      allPages.length < MAX_TICKET_PAGES
        ? (lastPage.nextCursor ?? undefined)
        : undefined,
    // Retries one page at a time, so a failed page doesn't refetch the rest.
    retry: retryTransientErrors,
    staleTime: TICKETS_STALE_TIME_MS,
  });

/**
 * All of the signed-in user's tickets, so the table can filter and page
 * through them. The first page shows as soon as it arrives; the rest load
 * one after another until the cursor runs out or `MAX_TICKET_PAGES` is
 * reached. A page that fails stops the loading, keeps what loaded, and can be
 * retried with `query.fetchNextPage()`.
 *
 * While the first page errors, the page falls back to the Messenger and Help
 * Center links, so it works before the endpoints are deployed.
 */
export function useSupportTickets() {
  const query = useInfiniteQuery(supportTicketsQueryOptions(useTicketOwner()));
  const { hasNextPage, isFetching, isFetchNextPageError } = query;
  const { fetchNextPage, dataUpdatedAt } = query;
  const pages = query.data?.pages;
  // Wait for any fetch to finish, including a refresh of stale pages: by
  // default `fetchNextPage` cancels it and pages on from the old cursor.
  // `dataUpdatedAt` re-runs this after every fetch, since a fast one can land
  // without a render that shows it fetching.
  React.useEffect(() => {
    if (hasNextPage && !isFetching && !isFetchNextPageError) {
      void fetchNextPage({ cancelRefetch: false });
    }
  }, [
    dataUpdatedAt,
    hasNextPage,
    isFetching,
    isFetchNextPageError,
    fetchNextPage,
  ]);
  return {
    query,
    tickets: pages?.flatMap((page) => page.data),
    /** More pages are on their way. */
    isLoadingMore: hasNextPage && !isFetchNextPageError,
    /** A later page failed; the tickets before it are still listed. */
    isLoadMoreError: isFetchNextPageError,
    /** Stopped at `MAX_TICKET_PAGES` with more tickets left. */
    isTruncated: !hasNextPage && Boolean(pages?.at(-1)?.nextCursor),
  };
}

/** Organizations rarely change while the form is open. */
const ORGANIZATIONS_STALE_TIME_MS = 5 * 60_000;

/**
 * The organizations the user can open tickets for: the ones they belong to,
 * which the global API checks with Frontegg. Until the API has this endpoint,
 * it errors and the form stays on the signed-in organization.
 */
export function useSupportOrganizations() {
  return useQuery({
    queryKey: supportTicketQueryKeys.organizations(useTicketOwner()),
    queryFn: async ({ signal }) => {
      const { data } = await listSupportOrganizations({ signal });
      return data.data;
    },
    retry: retryTransientErrors,
    staleTime: ORGANIZATIONS_STALE_TIME_MS,
  });
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
  return list?.pages
    .flatMap((page) => page.data)
    .find((ticket) => ticket.id === ticketId);
}

/** A ticket to open, and the files to add once it's open. */
export type CreateTicketVariables = {
  request: CreateSupportTicketRequest;
  files: File[];
};

export function useCreateSupportTicket() {
  const queryClient = useQueryClient();
  const owner = useTicketOwner();
  return useMutation({
    mutationFn: async ({ request }: CreateTicketVariables) => {
      const { data } = await createSupportTicket(request);
      return data;
    },
    // Runs even if the form closes or unmounts before the ticket opens, unlike
    // callbacks passed to `mutate`.
    onSuccess: ({ pagesOnCall: _, ...ticket }, { files }) => {
      // The ticket opens first, so files can never hold up an outage report.
      if (files.length > 0) {
        startAttachingFiles(queryClient, {
          ticketId: ticket.id,
          ticketNumber: ticket.number ?? null,
          files,
        });
      }
      const { queryKey } = supportTicketsQueryOptions(owner);
      // Intercom's ticket search takes a few seconds to include a new ticket,
      // so a refetch now would come back without it. Add it to the loaded
      // list and leave the updated cache fresh until its stale time expires.
      queryClient.setQueryData(queryKey, (list) => {
        if (!list) return list;
        const [first, ...rest] = list.pages;
        if (
          !first ||
          list.pages.some((page) =>
            page.data.some(({ id }) => id === ticket.id),
          )
        ) {
          return list;
        }
        return {
          ...list,
          pages: [{ ...first, data: [ticket, ...first.data] }, ...rest],
        };
      });
    },
  });
}

export type AttachFilesVariables = {
  ticketId: string;
  /** For messages about the files once the confirmation has closed. */
  ticketNumber: string | null;
  files: File[];
};

export type AttachFilesState = MutationState<
  AttachedSupportTicketFiles,
  Error,
  AttachFilesVariables
>;

/**
 * Retries only when the global API was busy and turned the files away
 * without reading them (503). Any other failure, a timeout above all, may
 * have added them, and a retry could add them twice.
 */
export const retryWhenUploadsAreBusy = (failureCount: number, error: unknown) =>
  failureCount < 2 &&
  error instanceof OpenApiFetchError &&
  error.status === 503;

/**
 * Whether an attach failure may mean the files still landed on the ticket.
 * A 504 is Cloud saying Intercom didn't confirm. A `TypeError` is a dropped
 * request after `readAsBase64` (FileReader failures are DOMExceptions).
 */
export const isAttachUnconfirmedError = (error: unknown) =>
  error instanceof OpenApiFetchError
    ? error.status === 504
    : error instanceof TypeError;

/** How to share files that couldn't be added. */
export const SHARE_FILES_INSTEAD =
  "Reply to the confirmation email or open the ticket in Help Center to share them.";

/** What became of a ticket's files, or nothing if it had none. */
export function attachFilesMessage(
  state: AttachFilesState,
  ticketLabel = "Your ticket",
): string | undefined {
  const count = state.variables?.files.length ?? 0;
  if (count === 0) return undefined;
  const files = `${count} ${pluralize(count, "file", "files")}`;
  if (state.status === "success") return `${files} added to your ticket.`;
  if (state.status !== "error") return `Adding ${files}\u2026`;
  return isAttachUnconfirmedError(state.error)
    ? `${ticketLabel} is open, but we couldn't confirm the ${files} ${pluralize(count, "was", "were")} added. Check the ticket in Help Center before adding them again.`
    : `${ticketLabel} is open, but the ${files} couldn't be added.`;
}

/**
 * Uploads outlive the components that start them, so their toasts go through
 * Chakra's shared toast store, which the app's ToastProvider renders, rather
 * than `useToast`.
 */
const { toast: showToast } = createStandaloneToast({
  defaultOptions: TOAST_DEFAULTS,
});

/**
 * Adds files to one of the user's tickets, as a reply from them. The files
 * are read and base64-encoded here, as the global API takes them.
 *
 * Runs in the mutation cache rather than a component, so it finishes even if
 * the form that opened the ticket closes. `useAttachFilesState` reads how it
 * went.
 */
export function startAttachingFiles(
  queryClient: QueryClient,
  variables: AttachFilesVariables,
) {
  const mutation = queryClient.getMutationCache().build(
    queryClient,
    queryClient.defaultMutationOptions({
      mutationKey: supportTicketQueryKeys.attachFiles(variables.ticketId),
      mutationFn: async ({ ticketId, files }: AttachFilesVariables) => {
        const encoded = await Promise.all(
          files.map(async (file) => ({
            name: attachmentFileName(file.name),
            data: await readAsBase64(file),
          })),
        );
        const { data } = await attachSupportTicketFiles(ticketId, encoded);
        return data;
      },
      retry: retryWhenUploadsAreBusy,
      // The global API asks for 5 seconds.
      retryDelay: (failureCount) =>
        (failureCount + 1) * 5 * millisecondsInSecond,
      // Keeps how it went, for the ticket's modal, for up to an hour (in
      // memory, so not across a page reload).
      gcTime: millisecondsInHour,
    }),
  );
  mutation.execute(variables).catch(() => {
    // The confirmation and the ticket's modal show the error too, but they
    // may have closed by now.
    const state = mutation.state as AttachFilesState;
    const message = attachFilesMessage(
      state,
      variables.ticketNumber
        ? `Ticket #${variables.ticketNumber}`
        : "Your ticket",
    );
    if (!message) return;
    showToast({
      status: "error",
      description: isAttachUnconfirmedError(state.error)
        ? message
        : `${message} ${SHARE_FILES_INSTEAD}`,
      // It needs acting on, so it stays until closed.
      duration: null,
      isClosable: true,
    });
  });
}

/** How adding files to a new ticket went, if any were added. */
export function useAttachFilesState(
  ticketId: string,
): AttachFilesState | undefined {
  return useMutationState({
    filters: {
      mutationKey: supportTicketQueryKeys.attachFiles(ticketId),
      exact: true,
    },
    select: (mutation) => mutation.state as AttachFilesState,
  }).at(-1);
}
