# Server State Management Migration Design Doc

## Related Issues

- **[Consecutive HTTP requests cause race condition on loading state](https://github.com/MaterializeInc/console/issues/146)**
- [Find a way to make network logs readable](https://github.com/MaterializeInc/console/issues/466)
- [Replace polling with websockets](https://github.com/MaterializeInc/console/issues/33)

## The Problem

There are specific features we want in the Console that are not possible or are too complex to implement with how we currently manage server data in the Console.

- **Caching**
  - In our sources list page, if we filter via the `jun_test` schema then remove filters, we show a loading spinner in between even though we've already fetched the initial state. This is more apparent for pagination when you flip between pages
- **Prefetching**
  - We want to be able to prefetch data (i.e. i want to fetch clusters AND the indexes when going into the Cluster overview page) without having to change too much code.
- **Retrying**
  - If we fail to fetch the sources, we want to retry via exponential backoff or another method before we show an error state.
- **Replace polling with websockets**
  - There’s no reason why we need a pull system(polling) when we could use a push system(subscribe with websockets).
- **Messy Websocket API**
  - The current WebSocket implementation can be improved since the control flow is hard to read (cluttered `useEffect` dependency array, automatic reopening when connection is closed but the environment is healthy, caching of accessToken)
  - Making this cleaner allows for a smoother transition to push-based real time updates
  - This isn’t required for the migration but isn’t a big lift and works well if we’re trying to replace polling anyways

### Current way of fetching data and why it doesn’t work

**Prefetching**

- Currently when we fetch data in the Console, we fetch it on mount of a component (i.e. `SourcesList.tsx`). The data is locally scoped to the Component and anything nested within `SourcesList` can access this data but anything out of scope (i.e. the navigation bar) can’t. So the solution is to hoist the data fetching to the common ancestor. However this means we can’t prefetch data since each fetch requires a mount of a React component and sometimes we want to prefetch via browser event handlers (i.e. `onClick`)

**Caching**

- We currently don’t cache data at all (we only have the current state of data) and the way we minimize the number of loading spinners is while we load the new state in our poll intervals, we delay showing the loading spinner if the new state comes back within 200ms. However, this makes it look like the operation didn’t do anything for the first 200ms.

**Retrying**

- We only retry when the authentication fails if our Frontegg token isn’t synced. However implementing a configurable retry system requires a revamp of our current server state manager. This is because our data model has hooks on top of hooks that each handle their state valuables separately. An example is:

```sql
usePoll
	useSqlTyped
		useSqlMany
			useSqlApiRequest
```

State transitions aren’t atomic. They’re also duplicated and split between hooks (such as `isPolling` from `usePoll` and `isLoading` from `useSqlApiRequest`). Due to this, we’d get some unpredictable/convoluted behavior when trying to implement retrying (how would we differentiate an `isRetrying` state value from an `isPolling` and `isLoading`)?

**Replace polling with websockets**

Similar to above, the current server state management is a bit too complex to integrate a push system without weird behavior.

## Success Criteria

<!--
What does a solution to this problem need to accomplish in order to
be successful?

The criteria should help us verify that a proposed solution would solve
our problem without naming a specific solution. Instead, focus on the
outcomes we hope result from this work. Feel free to list both qualitative
and quantitative measurements.
-->

A state management solution for server state that cleanly satisfies the following:

- Caching
  - A way to update the cache without being tied to the React lifecycle
  - Garbage collection of cached data
- Prefetching
- Retrying and configurable retry options per query
  - Adjusting the retry function
  - Custom logic based on the error we retry on
- Consistent state variables of the current status of a query (refetching, error, loading) from the server to integrate push-based live updates. Should answer the following questions:
  - Initial state:
    - Are we initially loading
    - Did we load initial data successfully
    - Did we error initially
  - Refetch state:
    - Are we refetching
    - Did the refetch fail
    - Did the refetch succeed
- Access to server side data outside of the React lifecycle
- Polling functionality
  - In case we still need to poll if we need to subscribe to data that’s unmaterializable
  - This can’t be implemented separate from the proposed system since we need these state transitions to be atomic for consistent behavior

## Out of Scope

<!--
What does a solution to this problem not need to address in order to be
successful?

It's important to be clear about what parts of a problem we won't be solving
and why. This leads to crisper designs, and it aids in focusing the reviewer.
-->

- Fetch data then code download via server side rendering
  - This feels like a premature optimization
- Micro-optimizations to make cached data seem fresh
  - We don’t need to trick users into thinking cached data is up to date. Especially if we assume our pages are updating realtime(via ws or polling). Fetches aren’t expensive for us and we can keep the current behavior of fetching a snapshot when we go to a page then updating the page in real time. We’re using caching as a cosmetic effect such that when you go between the Cluster list and the Sources list, you don’t get a loading spinner each time or a delayed transition. Thus we can treat all cached data as “[stale](https://www.google.com/search?q=stale+data&oq=stale+data&gs_lcrp=EgZjaHJvbWUyBggAEEUYOTIKCAEQABixAxiABDIKCAIQABixAxiABDIKCAMQABixAxiABDINCAQQABiDARixAxiABDIKCAUQABixAxiABDINCAYQABiDARixAxiABDIJCAcQABgKGIAEMg0ICBAAGIMBGLEDGIAEMgoICRAAGLEDGIAE0gEHNzkxajFqN6gCALACAA&sourceid=chrome&ie=UTF-8)”. This also avoids cache invalidation issues.
- Normalization of data in cache

  - Let’s say you fetch a list of posts(title, author) and fetch a more in-depth view of the post if you click into it(date_created, comments). Our cache shouldn’t need to normalize the post data like:

  ```
  sql
  post_id: {
    title,
    author,
    date_created
    comments: [commentId]
  }
  ```

  similar to other caching solutions like Relay or Apollo. This is because we don’t need to worry about cache invalidation issues (i.e. you create a new cluster and you need to update anything that fetched that cluster’s information) since all our data is considered stale anyways. This makes the desired caching solution and mutation code a lot simpler too.

- Multiplexing of websocket connections
  - After some testing, each browser can easily handle 25 concurrent websocket connections in a tab and persist them for an indefinite amount of time while the tab is active. In practicality, we’ll most likely only have a maximum of 3-5 concurrent websocket connections going at a time per tab. An example of this is the Shell where we have a websocket connection open for the schema select, database select, and the Shell itself. Additionally, a browser will automatically close a websocket connection if the tab is inactive for some period of time. Thus there’s no need to multiplex websocket connections on the client side unless environmentd needs to.
- Optimistic updates
  - For the migration, we don’t need to migrate our mutation code to incorporate optimistic updates. An example is if you create a cluster, we don’t need to manually update the cache containing the list of clusters since we’ll end up fetching the list when we see it again or it’ll be updating in real time. However the caching system should allow the option to do optimistic updates.
- List Pagination
  - We should have pagination, but that can be done later! This migration is meant for laying out the groundwork for pagination
- Integration testing for real time updates
  - Currently in our codebase, we don’t test our current real time strategy via polling. We also aren’t able to mock out our WebSocket connections, (due to lack of support from the Mock Service Worker library). This is something we might want to do in the future.
- Changing our current queries when not necessary
  - This migration doesn’t cover making queries more efficient or migrating them to Kysely. This would be nice however in the future.

## Solution Proposal

### Use the library React Query to fetch the initial/snapshot state then initiate a WebSocket connection for real time updates

React-Query is a popular, highly configurable JavaScript library that acts as a server state manager. It’s optimized for syncing server state and is decoupled from React. It handles caching, prefetching, retries, error states, loading states, refetching, optimistic updates, and garbage collection of the cache. The API is clear and fulfills all criteria above. It has a caching system that acts as the source of truth for server data.

**Example / General Algorithm:**

1. Go to the source list page. We need a separate SQL query for getting the list of sources, the list of schemas, and list of databases. Each of these lists will have an id associated with them. We create a cache key based on the ID and what parameters we use in the query. For example, if you have queries like `SELECT * FROM mz_sources;` , `SELECT * FROM mz_sources where name='postgres';`

   and `SELECT * FROM mz_sources where name='kafka';`, the id would be `source_list` and the cache keys in order will be something like `['source_list', '']` `['source_list', 'postgres']` and `['source_list', 'kafka']`

2. Fetch the snapshot state of sources, schemas, and databases using the queries built via Kysely. If the fetch was successful, we populate the cache with the fetched data via each query’s cache key. React Query automatically handles the caching, retries, and the state of the query here via its [query functions](https://tanstack.com/query/latest/docs/react/guides/query-functions).
3. Only when these fetches are successful and in a steady state (not re-fetching and erroring) do we initiate the WebSocket(WS) connection and start updating the data in real time.

   To update the data in real time:

   1. Issue the same query wrapped in a `SUBSCRIBE(withSnapshot=true, envelope upsert)` via the WebSocket connection.
   2. The WS connection will send messages of state transitions. We update the cache with the state transitions under the appropriate cache key. Each update to the cache will tell React to render. There are a couple ways we can do this:
      1. Update the cache per message. The caveat is if we have a paginated list and we get (delete row 2) then (upsert row 2 with ‘Bob’), the user will see the list drop in size then grow
      2. Have a buffer of each state transition then update the cache from the buffer based on a debounce.
      3. Do b) without a debounce and have some logic to handle the case in a)

   The WS connection will be closed on the following conditions:

   1. We fetch the initial snapshot again for whatever reason(tab unfocus → focus, offline to online)
   2. The server closes the connection
   3. The browser closes the connection

   If the WS connection closes for whatever reason, the client can choose to transiently ignore it, retry a number of times, or surface the disconnection via UI.

4. If you filter the list, we issue a new source list query with a new cache key, running steps 2→3 again.

Note: We don’t need to use the same query for the snapshot fetch and real-time updating. A case where this is important is when the snapshot query calls an unmaterializable function since `SUBSCRIBE` doesn’t work for unmaterializable functions. The two should be decoupled.

**Edge cases**

1. Someone switches regions:
   - We could add the current region to all cache keys, but that would result in users seeing data from the previous region when switching to the new region. Instead, we should clear the cache entirely whenever a user changes regions.
2. Syncing the Frontegg auth token:
   - Currently in our server state management system, we look out for any authorization errors then refresh our auth token then proceed to update the auth token embedded in the Frontegg part of our app. Then we retry the request
   - Because React Query allows us to handle retry behavior by error, we can do the same exact thing.
3. Can’t subscribe on our RBAC CTEs
   - Many of our object tables join on some RBAC CTEs which use `mz_is_superuser` and `current_user`. Because these functions are unmaterializable, we’re unable to port from polling to subscribe using the same queries. With the new system proposed, the way we would do this is our initial snapshot would query the object table and RBAC table, then SUBSCRIBE would just query the object table. The caveat is when we’re updating the cache, we need to map the result we get from the WS connection to the snapshot result set.
     Another approach to this is instead of including the RBAC CTE in each of our queries, we can query the RBAC CTE for all objects on initial load for the Console, then any client code that needs to hide UI due to RBAC we can check client side via the global privilege table. Then our snapshot query and real time query can be the same. The caveat is a slower initial load(depending on if we block the UI) and fetching more data than we need.

**Pre-work**

- Improve our WebSocket API

**Testing**

- The benefit to this is not only do our tests not need to change, but we can make them even better. We currently mock our queries by identifying each query by parsing out the query itself, extracting the columns, then matching on those columns. This is very brittle and we’ve decided we should replace that method by matching on a key instead. This plays in well since with this new system, we’re going to have a key per query anyways!
- We should unit test the new WebSocket API and the code connecting React Query and the WebSocket API.
- We should write an E2E test to make sure the behavior of real time updates works correctly. This should be a test similar to the MVP demo (source creation in real time)

<!--
What is your preferred solution, and why have you chosen it over the
alternatives? Start this section with a brief, high-level summary.

This is your opportunity to clearly communicate your chosen design. For any
design document, the appropriate level of technical details depends both on
the target reviewers and the nature of the design that is being proposed.
A good rule of thumb is that you should strive for the minimum level of
detail that fully communicates the proposal to your reviewers. If you're
unsure, reach out to your manager for help.

For the console, depending on the scope and complexity of the design,
some things you might want to include are:

- If/how this solution depends on other interfaces, such as the cloud and
database APIs. Are any changes required there?
- What libraries you're planning to use.
- What shared components you'll use, and what new components you'll add.
- Any refactoring or other pre-work required.
- How the work will be phased.
- Where and how feature flags will be used.
- Other considerations you might want to think about up front, like dark mode,
smaller screens, browser support, error handling, testing, and observability
that are more complicated than usual.

Some of the above may instead be covered in the MVP section below.
You can also create sub-sections in this section where useful to go into
more detail.
-->

## Minimal Viable Prototype

I created an MVP during a Skunkworks in [this PR](https://github.com/MaterializeInc/console/pull/1091). The PR contains pretty similar code to the final implementation and many TODOs.

## **Migration plan**

We plan to do this in the following phases:

### Set up the base

- Install React Query
- Clean up the WebSocket API and write the unit tests mentioned above
- Migrate our source lists page to this new system. Link this PR as documentation for how to migrate.
- Write documentation of how to use this new system.

This way any future features can use our new system.

### Replace non-polling queries

These are pages that don’t poll. Each of these can be broken into a separate PR/issue. An example is

- `SecretsList.tsx`

### Replace mutation pages

These are pages like our kafka source creation form. An example is

- `NewKafkaSource.tsx`

### Replace polling pages

An example is

- `SinkRoutes.tsx`

### Wrap-up

- Write the e2e test mentioned above

<!--
Build and share the minimal viable version of your project to validate the
design, value, and user experience. Depending on the project, your prototype
might look like:

- A Figma wireframe, or fuller prototype
- SQL syntax that isn't actually attached to anything on the backend
- A hacky but working live demo of a solution running on your laptop or in a
staging environment

The best prototypes will be validated by Materialize team members as well
as prospects and customers. If you want help getting your prototype in front
of external folks, reach out to the Product team in #product.

This step is crucial for de-risking the design as early as possible and a
prototype is required in most cases. In *some* cases it can be beneficial to
get eyes on the initial proposal without a prototype. If you think that
there is a good reason for skpiping or delaying the prototype, please
explicitly mention it in this section and provide details on why you you'd
like to skip or delay it.
-->

## Alternatives

### WebSockets for snapshot and real time updates

So the issue with the proposed solution is you can run into this case:

_Snapshot:_

| name  | age |
| ----- | --- |
| barry | 5   |
| alice | 2   |

_Events we miss between the HTTP fetching of the snapshot and setting up the WebSocket connection:_

| name  | age |
| ----- | --- |
| barry | 5   |
| alice | 1   |

| name  | age |
| ----- | --- |
| barry | 5   |
| alice | 3   |

_WebSocket sends back the snapshot_

| name  | age |
| ----- | --- |
| barry | 5   |
| alice | 3   |

Now you might be asking… If the WebSocket connection is already giving us the snapshot, why are we even using http from the get go? We can even use the `progress` option to derive the loading state! Then for caching, we can still use React Query and update the cache with our WS messages. The following reasons is why this would make the migration much harder:

1. We can’t use Mock Service Worker to mock our WebSocket connections. Thus all our current tests become moot and we need to write a centralized client for all our WebSocket connections that we can mock.
2. React Query’s handling of errors, retries, and automatic caching relies on transactions instead of incremental updates. We’d have to implement everything ourselves. There are also no libraries similar to React Query for websockets. This is because HTTP has a standard protocol while the closest thing WebSockets has is GraphQL’s Subscriptions and Socket.IO.
3. If we ever decide to move to server side rendering, all of its caching and data management relies on HTTP. Thus it would essentially require a rewrite.
4. If we enable `progress` for subscribe, we get ~3 null messages every second per connection. This makes debugging a nightmare.
5. If our goal is to not miss updates, this sollution won’t even guarantee that. Let’s assume we’re using a WebSocket connection and we’ve fetched half of the initial snapshot but we lose network connection, causing the connection to close. Based on the requirements, we would have to retry by recreating the connection. However between disconnecting and recreating the connection, we’ve potentially lost some state transitions.

Which leads me to ask… Does it matter if we miss updates? For our graphs, because we’re computing on historic data, it actually does matter. However, if our job is to show the present state of some data (like the tables above), we won’t care if Alice changed to 1 in such a small time frame since Alice=3 is now the correct state, the state we care about.

### Server-side rendering for snapshot + cache, client-side WebSocket connection for real time updates

Because the two biggest server side frameworks Remix and Next don’t support updating their server side caching system from client code, we would have to integrate a client side cache anyways.

## Open questions

<!--
What is left unaddressed by this design document that needs to be
closed out?

When a design document is authored and shared, there might still be
open questions that need to be explored. Through the design document
process, you are responsible for getting answers to these open
questions. All open questions should be answered by the time a design
document is merged.
-->

### For the adapter folk:

1. How many WebSocket connections are too many? Assume 1000 customers are using Materialize at the same time and they’re actively working on three tabs, each potentially using 3 WebSocket connections that run `SUBSCRIBE`, (3x3x1000 = 9000 connections). Is this a realistic case and is that okay for environmentd?
2. Should `mz_is_super_user` and `current_user` be materializable? If so, how long would this change take? Asking since it blocks the work of this migration (refer to the “can’t subscribe on our RBAC CTEs” section)
