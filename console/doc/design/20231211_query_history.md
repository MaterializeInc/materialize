# Query History

- Query history epic https://github.com/MaterializeInc/console/issues/952
- MFP pushdown with ILIKE fix https://github.com/MaterializeInc/materialize/issues/23755
- https://www.figma.com/file/l0gVOQOFhn8OGxxKjgCq0R/Query-History?type=design&node-id=1-7&mode=design&t=7BmOTmGxKlyA8lg8-0

## The Problem

- Users want to easily view queries, performance metrics, and metadata from the Console to identify slow queries, understand statement history, or identify query patterns.
- Users need a top-level view of queries for upcoming [Query Lifecycle features](https://github.com/MaterializeInc/materialize/issues/23314)

## Success Criteria

There are two use cases that field engineering (Chuck specifically) use the activity log table for:

### Recent query latency monitoring

1. He’s checking in on Vivian Health periodically. More-so **if any queries are slow**
2. Sees the past couple hours and sorts by latency

### Prolonged query latency monitoring

Not as frequently, Chuck will run [latency histogram and percentile queries](https://www.notion.so/Observability-Queries-ea2783099b6e42a18c169b154c1f88ba?pvs=21) over 7 days to see if anything is odd.

We need to create a UX that fulfills the recent query latency monitoring and can be extended for some prolonged latency monitoring. The UX should not OOM the `mz_introspection` cluster and should feel as performant as possible.

## Out of Scope

- Creating an in-depth system for the prolonged monitoring case. This is because most customers want to set these systems up via Grafana or other tools instead. Vivian Health actually tried to but OOM’d their cluster instead
- Building the [Detail page](https://www.figma.com/file/l0gVOQOFhn8OGxxKjgCq0R/Query-History?type=design&node-id=354-4566&mode=design&t=7BmOTmGxKlyA8lg8-0) since its usefulness is limited while Query Lifecycle features are being fleshed out
- Incorporating filters that aren’t currently exposed by `mz_recent_activity_log`
- Hooking subscribe up to the view
- Creating aggregated views for queries with the same explain plan

## Solution Proposal

Let _OOM_RANGE_ be the interval of time between such that all users can query the `mz_recent_activity_log` table without OOMing the cluster.

We want to build a page of a table that allows a user to

- Select certain columns to display
- Use filters to narrow in on certain queries
- Sort the table

### **Filters we want to support:**

- Session id
- Cluster
- Search-bar that uses the Postgres ILIKE function
- Date range
  - An interval of time that's bounded by _OOM_RANGE_
  - A user can select a point in time called _end_. Let _start = end - OOM_RANGE_. They can look at data from _start_ to _end_. A user can choose _end_ by the minute
- Email / user
- Application name
- Status (Running, success, failed)
- Statement type (select, create, drop, alter, subscribe, explain)
- Duration

### **Sort options:**

- Start time
- End time
- Duration
- Status

### **High level architecture:**

**Filters:**

We’ll use React hook form or a custom reducer for setting up the filters to a state object and for validation

**UI Components**

We’ll use our current UI components for our form inputs and we’ll have to create a custom date picker.

For the custom date picker, [datepicker](https://github.com/rehookify/datepicker) seems like a good choice to start off with. It’s completely headless and exposes date operations and state management. The markup doesn’t seem hard to implement and with it being headless, it gives us full control over styling. The library is also written in TypeScript and has a small bundle size. If needed, we can always fork it if we need to configure the state management. It also has a good number of examples.

For the table, we’ll need to update the styling as well. We won't be implementing pagination or infinite scrolling for the table for v0 given it doesn't provide much functionality with the current set of filters. Instead, we'll set a max number of rows per query and if they scroll to that point, we'll show a notice saying something like "limit reached, try refining your query". If there is demand for this, we can implement cursor-based pagination in the future.

**Data fetching**

Using the ephemeral state object of our filters, we create the query using Kysely. We will fetch data and control the data layer with React-Query. The query key will be comprised of a namespace and all variables use to create the query.

We’ll also fetch all columns rather than the subset of columns hidden/shown since otherwise, hiding/showing columns would result in a new fetch which would result in poorer performance.

An example of an SQL query using all the filters:

```sql
SELECT
  mal.application_name,
  mal.began_at,
  mal.cluster_name,
  mal.execution_id,
  mal.execution_strategy,
  mal.finished_at,
  mal.finished_status,
  mal.rows_returned,
  mal.session_id,
  mal.sql,
  msh.authenticated_user,
  mal.finished_at - mal.began_at AS duration
FROM
  mz_internal.mz_recent_activity_log AS mal
  JOIN mz_internal.mz_session_history AS msh ON mal.session_id = msh.id
WHERE
  -- Date range filter
  prepared_at >= ${end} - interval ${oom_range}
  AND prepared_at <= ${end}
  AND began_at >= ${end} - interval ${oom_range}
  AND began_at <= ${end}
  -- Cluster filter
  AND mal.cluster_id IN ${cluster_ids}
  -- Session ID filter
  AND session_id = ${session_id}
  -- Finished status filter
  AND finished_status IN ${finished_status}
  -- Email/user filter
  AND authenticated_user = ${authenticated_user}
  -- Application name filter
  AND application_name = ${application_name}
  -- Duration filter
  AND finished_at - began_at >= ${MIN_DURATION} MILLISECONDS
  AND finished_at - began_at <= ${MAX_DURATION} MILLISECONDS
  -- Statement type filter
  AND statement_type ILIKE '${statement_type}%'
-- Sort filter
ORDER BY
  ${sort_filter_ref} ${sort_order}
LIMIT ${max_num_rows};

```

```bash
${end} = TIMESTAMP '2023-12-07T16:58:00+00:00'
${oom_range} = '1hour'
${cluster_ids} = ('u1')
${session_id} = '21228060-0d9e-40a0-8c90-cb7da2704863'
${finished_status} = ('success')
${authenticated_user} = 'jun@materialize.com'
${application_name} = 'web_console_shell'
${MIN_DURATION} = '200'
${MAX_DURATION} = '300'
${sort_filter_ref} = finished_at - began_at
${sort_order} = DESC
${statement_type} = 'create'
${max_num_rows} = 1000
```

This query doesn't include the search string filter and statement type filter.

In order to get some of these parameters such as `${cluster_ids}`, we'll need to do a separate fetch call.

**RBAC**

Currently, certain roles like `mz_support` cannot query `mz_recent_activity_log` since some information like the sql text is private. There are "redacted" versions however that replace parameters, the sensitive part, with "<REDACTED>". To handle this, we'll create a unified interface schema definition between the two tables then choose which table to use on runtime based on the user's privileges.

### Search Parameter API

We'll want to store the user-set filter options in the URL similar to Grafana. This is for better persistence of session data.

The URL parameters should all be of type string or a list of strings. The query parameters will be encoded with UTF-8 in the URL. We can do some client side validation for some query parameters in case they're an incorrect value and unset them if they are.

#### Decoded Query Parameters

- **date_range[]** _(optional, default: [current time - 3 hours, current time])_: A 2-tuple where the first element is the minimum date to query from and the second element is the maximum date, inclusive for both. Each element is an ISO date string. An example is `"date_range[]=2023-12-29T02:15:06.316Z&date_range=2023-12-30T02:15:06.316Z"`.
- **cluster_id** _(optional, default: No filter on cluster id)_: A cluster id to filter on. An example is for `"cluster_id=u1"`, we'll find all queries that were run on the `u1` cluster.
- **session_id** _(optional, default: No filtering on session id)_: A session id to filter on.
- **finished_statuses[]** _(optional, default: No filtering on finished status)_: A list of finished statuses to filter on. A finished status can be `"running"`, `"error"`, `"canceled"`, `"aborted"`, or `"success"`. An example is for `"finished_statuses[]=running&finished_statuses[]=error"`, we'll find all queries that are running or failed.
- **show_console_introspection** _(optional, default: `false`)_: A boolean value to decide if we want to show queries ran by the application named `web_console`.
- **statement_types[]** _(optional, default: No filtering on statement type)_: A list of statement types to filter on. A statement type can be `"select"`, `"create"`, `"drop"`, `"alter"`, `"subscribe"`, or `"explain"`. An example is for `"statement_types[]=select&statement_types[]=drop"`, we'll find all queries that are of type "DROP" or "SELECT.
- **user** _(optional, default: Filter on the current user)_: A user's email to filter on. If we want to have no filter, we use `*`. Some examples of possible values are `"jun@materialize.com"` or `"*"`.
- **application_name** _(optional, default: No filtering on application name)_: An application name to filter on. An example is `"psql"`.
- **duration_range.min_duration** _(optional, default: No lower bound filtering on duration)_: The minimum duration in milliseconds to filter on (inclusive). An example is `"duration_range.min_duration=100"`. We filter on queries with durations >= 100ms.
- **duration_range.max_duration** _(optional, default: No upper bound filtering on duration)_: The maximum duration in milliseconds to filter on (inclusive). An example is `"duration_range.max_duration=1000"`. We filter on queries with durations <= 1000ms.
- **sort_field** _(optional, default: `"start_time"`)_: Specifies the column to sort on. Possible values are either `"start_time"`, `"end_time"`, `"duration"`, or `"status"`.
- **sort_order** _(optional, default: `"desc"`)_: Specifies the sort order for the results. Possible values are either `"desc"` or `"asc"`.
- **columns** _(optional, default: `"desc"`)_: Specifies the sort order for the results. Possible values are either `"desc"` or `"asc"`.

An example of the default URL:

Decoded: `/?date_range[]=2023-12-29T02:15:06.316Z&date_range[]=2023-12-30T02:15:06.316Z&sort_field=start_time&sort_order=desc`

Encoded: `/?date_range%5B%5D=2023-12-29T02%3A15%3A06.316Z&date_range%5B%5D=2023-12-30T02%3A15%3A06.316Z&sort_field=start_time&sort_order=desc`
``

### Performance concerns

Time vs. number of rows from `SELECT COUNT(*) FROM mz_recent_activity_log`:

| Number of rows in mz_recent_activity_log | Time    |
| ---------------------------------------- | ------- |
| 30,000                                   | 1.46s   |
| 35,465                                   | 2s      |
| 73,279                                   | 3.5s    |
| 91,420                                   | 5s      |
| 124,546                                  | 7.06s   |
| 147,162                                  | 8.10s   |
| 210,151                                  | 11s     |
| 239,124                                  | 16s     |
| 346,138                                  | 15.08s  |
| 370,728                                  | 21.70s  |
| 409,674                                  | 20s     |
| 496,683                                  | 21.21s  |
| 711,573                                  | 46.26s  |
| 1,656,590                                | 105.30s |

In this example, there are approximately 1.6 million rows generated by `SQLSMITH` in `mz_recent_activity_log`. From this table, it seems like the time increases somewhat linearly with the number of queries/rows.

However, there seems to be a lower bound of ~2.5 seconds when using a temporal filter even when there’s a low number of rows:

```sql
select count(*) from mz_internal.mz_recent_activity_log
  WHERE prepared_at > now() - interval '1hour'
    AND began_at > now() - interval '1hour';
```

| count |
| ----- |
| 36    |

> Returned in 2.4 seconds.

As well as a ~5 second delay when the query is first run.

The easiest things we can do to improve performance are:

- Index a view of the query with default filter parameters
- Create a limit on the number of rows returned per query and using infinite scroll. Load more when the user scrolls.
- Prefetch the on dropdown closes or mouseovers

We’ll need to experiment with performance depending on what _OOM_RANGE_ is. We do not want to prematurely optimize.

### Phasing

- Set up routing and a feature flag called `query-history-952`
- Create a query builder that accepts the date range filter and the search text filter. Hook the query builder with React Query to have the model / state working
  - Each filter should correspond to a different `WHERE` condition while each sort option corresponds to a different `ORDER BY` parameter
- Create a date picker component (will be shared with the Billing UI). Hook it up to the date range filter
- Hook a search bar to the search text as well as any other UI components (buttons, etc.)
- Create custom table styling for different overflow behavior and infinite scroll UI.
- Implement the rest of the filters and sort options
- Implement hiding/showing of columns
- Improve performance where needed

## Alternatives

### **Virtualized list**

A virtualized list can be used if we suspect a user to have hundreds / thousands of rows rendered at the same time. This is unlikely however since it would imply the user scrolling endlessly without setting a filter. Using a virtualized list also removes native webpage search behavior which may be more useful than perceived performance. If the number of DOM nodes end up being a performance bottleneck, we can revisit virtualizing the list.

### Alternatives date-picker libraries I looked into:

- [React-Aria useDatePicker](https://react-spectrum.adobe.com/react-aria/useDatePicker.html)
  - Made by Adobe
  - Small bundle size
  - Uses a different timezone library
- [react-datepicker](https://github.com/Hacker0x01/react-datepicker/tree/main)
  - Most popular React date picker library
  - Can customize CSS but cannot customize markup
  - Large bundle size

## Open questions

- How do we want to display multiline SQL queries? For example:

```sql

with

mutually recursive ...
```

would show only “with” in the table

Answer: We strip newlines out from the text

- Should we show only redacted SQL? What’s the benefit of knowing the parameters of each query other than visibility?

Answer: Queries with different parameters can affect performance. Take this query for example:

```sql
SELECT *
FROM Y
WHERE Y.name IN (
 SELECT name FROM X where X.name ILIKE $1
)
```

The performance actually depends on what `$1`` is.

- What is _OOM_RANGE?_
  - The best way to approximate this is to impersonate into the environment with the largest number of queries and calculate row count of `mz_recent_activity_log` on different periods of time.
- Do we want to have a set of default filters or save a user’s preference of default filters? Should we do this via cookies or save this metadata somewhere?

Answer: We should use URL parameters for persistence and consider user set default filters if demand increases.
