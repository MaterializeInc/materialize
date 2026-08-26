# Unified MCP Server and Permission Model

- Associated:
  [DEX-97](https://linear.app/materializeinc/issue/DEX-97/design-doc-unified-mcp-server-and-permission-model),
  [DEX-98](https://linear.app/materializeinc/issue/DEX-98/get-a-demo-claude-org-for-mcp-testing),
  [DEX-99](https://linear.app/materializeinc/issue/DEX-99/close-the-two-open-questions-from-the-org-wide-mcp-prd),
  [Enterprise MCP PRD](https://app.notion.com/p/materialize/Enterprise-MCP-3a513f48d37b80cfa5f5db1942024d09?source=copy_link)

Scope. The PRD covers five things: one server, RBAC, the newest protocol
revision, an audit table, and docs. This doc covers only the first two, because
that is where the design is not yet settled. The protocol uplift already has its
own project and plan. The audit table and the docs are handled as their own
tickets.

## The Problem

We ship two MCP servers. `/api/mcp/agent` serves user data products.
`/api/mcp/developer` serves the system catalog, and also carries a `query` tool
that reaches any object the role can read, user objects included. Between them
they expose five tools.

An admin who wants to turn Materialize on for a whole organization cannot answer
a simple question: what can this person do? The answer is split across two URLs,
and the split does not follow anything the admin controls. It does not even
split user data from the system catalog, since the developer endpoint reads user
objects too. It follows how we happened to build the endpoints.

The two servers also differ in ways that are invisible from outside:

- The developer endpoint's `query_system_catalog` tool pins `search_path` to the
  system schemas before it runs a query. No other tool does, including the
  developer endpoint's own `query` tool.
- The agent endpoint drops `cluster_replica` even if a client sends it. The
  developer endpoint honors it.
- Each endpoint returns its own `initialize` instructions.

So the two endpoints do not differ only by tool list. They differ by behavior. An
admin has no way to see that, and we have no way to explain it in one sentence.

## Success Criteria

- An admin hands out one URL, and that URL is the same on Cloud and on
  self-managed apart from the host.
- An admin can find out what a role is allowed to do without reading our source
  code.
- Clients pointed at the old URLs keep working with no change on their side
  through the deprecation window, and old routes are retired on evidence of
  disuse rather than on a date.
- Nothing the system catalog guard protects against today becomes reachable.
- We do not add a second permission system. Access stays governed by the grants
  and role settings a customer already has.

## Out of Scope

- **The 2026-07-28 protocol revision.** Tracked separately in the MCP
  2026-07-28 dual-era support project. Nothing in this design depends on it, and
  shipping this without it breaks no client, because current clients fall back to
  the older revision on their own.
- **The `mz_mcp_tool_calls` audit table.** Different code area, tracked
  separately, and it can be built at the same time as this. One note for
  whoever picks it up: it should not reuse statement logging as is, because
  that path is sampled and rate limited, so it drops rows under load, and an
  audit trail that drops rows cannot answer who read what.
- **The plugin and the marketplace listing.** Packaging work, no design risk.
- **Write tools.** Read-only for now, as the PRD says.
- **Narrower OAuth scopes.** Listed in the PRD as later work.
- **SCIM and the bundled Ory auth server.** Both are needed for the full user
  journey, but neither blocks anything here. We can build and test the server
  without them.

## Solution Proposal

Ship one route, `/api/mcp`, backed by one implementation, exposing three tools.
Point the two old routes at that same implementation, with a small per-route
list of which tools they may show. Keep the old tool names working as aliases.
Deprecate the tools that do not carry over and age them out over several
releases. Do not filter the tool list by role.

### One route, one implementation

```
https://<region-id>.materialize.cloud/api/mcp    Cloud
https://<host>:6876/api/mcp                      Self-managed
```

`/api/mcp/agent` and `/api/mcp/developer` stay in place and call the same code.
They are not a second implementation. This matters because the alternative,
keeping the old endpoints on their old code, means carrying two shapes of the
tool list until we delete one.

### A per-route tool list

If the old routes simply forwarded to the new code, a client connected to
`/api/mcp/agent` would suddenly see tools it never saw before. That is a
surprise we do not need to create.

So each route carries a short list of the tools it may show, and the name each
one is advertised under. The new route shows the new names. The old routes show
what they show today, under the names they use today. The list is a small piece
of config on top of one implementation, not a fork of the logic.

Both MCP endpoints are marked as public preview in our docs, so we are not bound to
keep the old shape forever. The list is there to avoid a surprise, not to
promise a contract.

### Tool names

`/api/mcp` exposes three tools.

| New name | Today | What it is for |
| --- | --- | --- |
| `list_data_products` | `get_data_products` | what data can I reach |
| `get_data_product_details` | same | explore one of them |
| `query` | same | run read-only SQL |

Old names keep working as aliases so no client has to change. On `/api/mcp` an
alias is accepted on `tools/call` but is not advertised in `tools/list`, so the
list stays at the three names above and an alias costs a client no context.

Aliasing and the per-route tool list are separate. Aliasing is a name mapping
and applies on every route. The per-route list decides which tools a route
advertises and under which name. A client on an old route therefore sees its
historical tools under its historical names, while the same old name also works
on `/api/mcp`.

Three tools do the work the five did. `query` already reaches anything the role
can read, system catalog included, so a separate catalog tool adds a second way
to do one thing. `list_data_products` already answers "what data can I reach",
which is the question a capability tool would have answered for the agent
surface.

Two tools do not carry over:

- `query_system_catalog`. Superseded by `query`. Callers name system objects in
  full rather than relying on an unqualified `mz_*` shorthand.
- `read_data_product`. Already being retired under DEX-65.

Neither is removed on the day `/api/mcp` ships. Both stay on the old routes,
marked deprecated, and age out over several releases as the replacements land.
The endpoints are public preview, so we are free to make the break. Doing it
with a migration window is a choice, not an obligation.

### The system catalog guard retires with the tool

The developer endpoint's `query_system_catalog` pins `search_path` to the system
schemas before running a query. That is the fix for database-issues#11320:
without it, a user who can create objects adds a view called `public.mz_leak`,
and an unqualified `mz_leak` inside a system query resolves to that view.
`test_mcp_developer_search_path_defense` covers it.

The guard exists only because that tool accepts unqualified `mz_*` names as a
shorthand. Dropping the tool drops the shorthand, and the hijack has nothing to
aim at. `/api/mcp` therefore needs no guard, because `query` never promised that
convenience and callers name system objects in full.

The guard stays on the old route for as long as `query_system_catalog` does, and
goes when it goes. Until then the existing test is what keeps it alive, since a
straight merge of the two routers would drop it without failing to compile.

### The other endpoint differences

The guard is not the only behavior tied to the endpoint. Two more are, and one
implementation has to answer both.

**`cluster_replica`.** The agent endpoint drops it, the developer endpoint
honors it. Replica pinning is what `EXPLAIN ANALYZE` needs on a cluster with
more than one replica, so `query` on `/api/mcp` honors it. Honoring it is not a
way around RBAC, because the role still needs USAGE on the cluster. The agent
route keeps dropping it, through the same per-route config that holds its tool
list, so clients there see no change.

**`initialize` instructions.** Each endpoint returns its own text, and that text
already varies by feature flag. `/api/mcp` needs its own version covering the
full tool list. The old routes keep theirs. This makes the instructions another
per-route value, not a new mechanism.

Session tagging and metrics also key off the endpoint. MCP sessions set
`application_name` to `mz_mcp_agents` or `mz_mcp_developer`, and the request
metric is labeled `agent` or `developer`. `/api/mcp` needs a third value for
both. It should not reuse either old name, because then nothing downstream could
tell which route a session came through. Anything that reads
`mz_session_history` by `application_name`, or charts the metric by label, has
to learn about the third value.

### Do not filter the tool list

The PRD asks for `tools/list` to return only the tools the caller can use. We
should not do that, for two reasons.

**It cannot be done from grants alone.** Most system catalog relations are
granted `SELECT` to `PUBLIC`, through the `PUBLIC_SELECT` item each builtin opts
into in `src/catalog/src/builtin.rs`. That is on purpose. Postgres makes
`pg_catalog` readable by everyone, and psql, drivers and ORMs all read it when
they connect. Taking that grant away to make "this role may not read the
catalog" a real grant would break that compatibility.

A few relations do opt out, so the ACL model is not uniform here.
`mz_catalog.mz_role_auth` and `pg_catalog.pg_authid` are owner-only,
`mz_internal.mz_catalog_raw` is reachable only by the system role, and some
history relations are limited to monitoring and support roles. Those are the
relations whose contents are sensitive on their own. They are the exception, and
they do not give us a grant that covers the catalog as a whole.

Because the grant model cannot say it, the guard was built as a separate check.
`check_restrict_to_user_objects` in `src/sql/src/rbac.rs` runs next to the
privilege checks, but it is not a privilege. It is an allow list over catalog
item types, plus a small list of exempt OIDs. Function and type catalog items
pass because queries need them. Everything else system owned is blocked. See
`doc/developer/design/20260508_restrict_to_user_objects.md` for that design.

That check is not the whole restriction. Dependencies inside SQL function bodies
go through it separately, unmaterializable functions have their own allow list
that is enforced during optimization in `src/adapter/src/optimize/dataflows.rs`,
and restricted `EXPLAIN ANALYZE` also requires ownership. So a function passing
the item type check does not mean every function call is allowed.

So the flag sits inside the RBAC module without being part of the ACL model, and
it cannot become part of it without leaving Postgres behavior behind.

Filtering by role would therefore ask a different question per tool, and for the
query tools it is not even a single question. `check_usage` in
`src/sql/src/rbac.rs` applies the restriction first and then the ordinary ACL
checks, so whether a call succeeds depends on the statement, the objects it
names, the normal grants, and the restriction on top. There is no one bit to
read per tool. Calling it RBAC filtering would be inaccurate.

**Filtering is not a security control.** RBAC is enforced when the tool runs, no
matter what `tools/list` returned. Hiding a tool does not protect anything. It
only changes what the model sees.

Nor do we add a tool that reports capability instead. An earlier draft had one,
returning grants and session settings so an agent could ask up front. It is cut.
Three tools that do work are easier to reason about than three plus one that
describes them, and the description would have needed its own upkeep as the
permission model moved underneath it.

What an agent gets instead is the data path it already has.
`list_data_products` returns what the caller can reach, which is the question
that tool would have answered for the agent surface. Beyond that the agent
learns by calling and reading the error, the same way a person learns at a psql
prompt.

This is the weaker half of the argument and worth naming. An agent that
discovers a limit by failing spends a turn on it, and our errors do not always
distinguish "you may not see this" from "this does not exist". If the prototype
shows agents handling that badly, either the errors need to carry more, or a
capability tool comes back. That is one of the things the prototype is for.

### How an admin controls access

Nothing here adds a way to grant or revoke a tool. Access is controlled in
three places that already exist, and the tools simply run as the caller.

**The operator, for the whole environment.** The `enable_mcp_*` dyncfgs turn
each endpoint, and each optional tool on it, on or off. They apply to everyone,
not to a role. `/api/mcp` gets its own flag on the same pattern,
`enable_mcp_unified`, so the three routes roll out independently. A bare
`enable_mcp` would read as a master switch over all three while only gating one,
which is worse than a slightly long name. Once the old routes are gone their
flags go with them, and the survivor can be renamed then.

**The admin, per role.**
`ALTER ROLE analyst SET restrict_to_user_objects = true` confines that role to
user objects and the MCP data product views. Only a superuser can set it, and
a plain `SET` of it is refused, so a role cannot lift the restriction for
itself and neither can a query the agent writes.

**The admin, per object.** The grants they already write: `SELECT` on what the
role should read, `USAGE` on the cluster the query runs on.

So "give this analyst a narrow agent" is one `ALTER ROLE` plus the usual
grants. Nothing in the tool surface changes that. The tools run as the caller
and get whatever those grants allow.

Confining a role to user data works:

```sql
CREATE ROLE app;
GRANT USAGE ON CLUSTER prod TO app;
GRANT SELECT ON TABLE sales.orders TO app;
ALTER ROLE app SET restrict_to_user_objects = true;
-- query reads sales.orders on cluster prod. The same tool against a system
-- catalog object fails, because the restriction rejects those and only the MCP
-- data product views are exempt.
```

The reverse takes more work and is not quite the same thing. A role with no
`USAGE` on any cluster still reads the catalog, because a catalog only statement
is auto routed to `mz_catalog_server` whatever cluster was asked for, and that
cluster grants `USAGE` to `PUBLIC`. The same role reaches no user data, since a
read of a user table resolves to the named cluster and needs `USAGE` there. So
the effect is reachable, but it starts with
`REVOKE USAGE ON CLUSTER quickstart FROM PUBLIC`, which is environment wide, as
the default cluster is granted to `PUBLIC` when the environment is created.

What is not reachable is withholding a tool. The role can still call `query`,
it just gets back what its grants allow. Access is per object, not per tool, so
two roles cannot be given different tool sets. With three tools that matters
less than it did, since `query` is the only one where the distinction would
have bitten, but it is still the subject of an open question below.

### Discovery metadata

MCP clients fetch RFC 9728 protected resource metadata before they have a token.
We serve it at three paths today: the bare
`/.well-known/oauth-protected-resource`, plus a path-suffixed alias for each
endpoint per RFC 9728 section 3.1. All three are mounted on the same handler and
return the same document.

`/api/mcp` needs its own alias at
`/.well-known/oauth-protected-resource/api/mcp`, for clients that always probe
with a suffix instead of falling back to the bare path. Because the document is
identical for every endpoint, this is one more route on the handler we already
have, not a second document to keep in sync. The two old aliases stay so clients
on the old URLs keep working.

### What changes for existing users

- Clients on the old URLs keep working. They see the same tools under the same
  names.
- Clients that move to `/api/mcp` see three tools under the new names.
- `read_data_product` is deprecated and goes away under DEX-65.

The old routes should not live forever. Both are marked public preview, so we
are not bound to keep them, but "not bound" is not a plan. The path:

1. Measure. The request metric is already labelled by endpoint, so usage of each
   old route is visible without new instrumentation.
2. Announce. Say it in release notes, and add a line to the `initialize`
   instructions the old routes return, which is the one place every client
   reliably reads.
3. Remove, once the metric shows the route is quiet and a release has passed
   with the notice in place.

No date here. The point is that a route is retired on evidence that nobody is
using it, not on a guess, and the metric is what supplies the evidence.

## Minimal Viable Prototype

Build `/api/mcp` behind a feature flag, serving the merged tool list, with the
two old routes pointed at the same code. Leave the audit table and the protocol
uplift out.

Then run the admin flow end to end in a real Claude organization: add the
connector once as an admin, and connect as two users with different roles.

What we are trying to learn:

1. Can an admin answer "what can this person do" from one URL and the grants
   they already wrote, or do they still need to read docs?
2. Does an agent behave well when it can see a tool it is not allowed to use? If
   it retries in a loop or gives a confusing answer, that is an argument for
   filtering after all, and we would revisit the decision above.
3. Does each user's session carry their own role?

Point 3 needs a Claude organization we control, tracked in DEX-98. That takes
time to arrange, so it starts now and runs alongside this design.

## Alternatives

**Filter `tools/list` using grants.** Rejected. The bulk of the system catalog is
readable by `PUBLIC` for Postgres compatibility, so the grant model cannot
express "this role may not read the catalog". See the solution section above.

**Add a privilege type for MCP tools**, such as
`GRANT MCP TOOL query TO analyst`. Rejected. A new privilege type
means new syntax and new durable catalog state, and it would not replace object
grants or the restriction flag, so it adds rather than simplifies. Making each
tool a builtin catalog object and granting `USAGE` on it is a different
proposal, since it reuses machinery we already have. That one is open, see the
open questions.

**Keep the old endpoints on their old code**, as the PRD's migration section
describes. Rejected. It means two tool list shapes to maintain until we remove
one, for no gain over a per-route list on top of shared code.

**Keep two servers and document them better.** Rejected. It does not fix the
admin's problem, which is that the split follows our implementation and not
anything they control.

**Control access with read-only flags or URL parameters.** Rejected, and the PRD
agrees. Flags set by the client cannot be enforced by the organization, and they
would be a second permission system next to the grants customers already use.

## Open questions

1. **Does the admin flow remove the per user OAuth step?**
   Mostly answered. Anthropic's support docs say that once an owner adds a
   connector to a Team or Enterprise organization, "users individually connect to
   and enable that connector", and that each user grants permission on their own
   behalf. So the documented behavior today is a per-user token, which is what our
   role mapping needs. The PRD says there is no individual OAuth flow. That does
   not match the support docs, and it does not match the PRD's own Scenarios 2
   and 3, which both show a per-user step.

   What is left is a product question, not a technical one: whether a newer
   enterprise feature removes the consent click. Either way each user still has
   their own identity, so this design does not depend on the answer. Confirming
   it end to end is DEX-98.
2. **Will our authorization servers support CIMD?**
   CIMD is part of the 2026-07-28 revision, on the authorization page, at SHOULD
   level. Dynamic Client Registration is marked deprecated there and kept only
   for authorization servers that do not support CIMD. The client registration
   order is pre-registration first, then CIMD, then DCR as a fallback.

   The work is not ours. With CIMD the client hosts its own metadata document and
   the authorization server fetches it. We are the resource server, so we do
   nothing. It lands on Frontegg for Cloud and on the bundled Ory Hydra for
   self-managed, and an authorization server advertises it with
   `client_id_metadata_document_supported` in its metadata.

   The open part is whether either of them supports it. The draft is at revision
   00, so Hydra probably does not yet. That is a risk for the self-managed plan
   in the PRD, which is built on DCR. It still works, because clients may fall
   back to DCR, but it builds the new story on the mechanism the spec is moving
   away from. This sits with whoever owns those two servers rather than with this
   project.
3. **Do the old routes really need their own tool list?** If the prototype shows
   agents handle unexpected tools well, we could point the old routes at the full
   list and drop the per-route config.
4. **Can tool access be made per role?** Today it cannot, as the section above
   describes. One option raised in review is to make each tool a builtin catalog
   object and grant `USAGE` on it, which reuses the grant machinery rather than
   adding a privilege type. Whether we want tools in the catalog at all is a SQL
   layer decision rather than an MCP one, so it needs that team to weigh in
   before this doc takes a position.
