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
- Clients pointed at the old URLs keep working with no change on their side.
- The system catalog guard is no weaker than it is today.
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

Ship one route, `/api/mcp`, backed by one implementation. Point the two old
routes at that same implementation, with a small per-route list of which tools
they may show. Keep the old tool names working as aliases. Move the system
catalog guard from the endpoint onto the tool that needs it. Do not filter the
tool list by role. Report what the caller may do through a tool instead.

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

So each route carries a short list of the tools it may show. The new route shows
everything. The old routes show what they show today. The list is a small piece
of config on top of one implementation, not a fork of the logic.

Both MCP endpoints are marked as public preview in our docs, so we are not bound to
keep the old shape forever. The list is there to avoid a surprise, not to
promise a contract.

### Tool names

| New name | Today |
| --- | --- |
| `get_permissions` | new |
| `get_settings` | new |
| `list_data_products` | `get_data_products` |
| `get_data_product_details` | same |
| `query` | same |
| `query_system_tables` | `query_system_catalog` |

Old names keep working as aliases so no client has to change. An alias is
accepted on `tools/call` but is not advertised in `tools/list`, so the list
stays at the six names above and an alias costs a client no context.
`read_data_product` is deprecated and is not in the new list. DEX-65 removes
it.

### The system catalog guard moves to the tool

The developer endpoint's `query_system_catalog` tool pins `search_path` to the
system schemas before running a query. Its `query` tool does not, and neither
does the agent endpoint. That is not a style choice. It is the fix for
database-issues#11320. Without it, a user who can create objects can add a view
called `public.mz_leak`, and an unqualified `mz_leak` inside a system query
resolves to that view.
`test_mcp_developer_search_path_defense` covers this.

The guard exists because `query_system_catalog` lets a caller write unqualified
`mz_*` names as a convenience. That convenience belongs to the tool, not to the
URL. So the guard moves with it, onto `query_system_tables`.

This is the part of the change most likely to be lost by accident, because a
straight merge of the two routers would drop it without failing to compile. The
existing test is what catches it.

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

### Report what the caller may do, do not filter the tool list

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

Instead we add two tools:

- `get_permissions` returns the grants the caller holds.
- `get_settings` returns the role settings that change what the caller can do,
  starting with `restrict_to_user_objects` and the default cluster.

The agent asks once and learns what it may do, and why something is not
available. Filtering would instead push an authorization decision into every
`tools/list` call, which every client makes when it connects. That is per-call
work we would be adding. It would not need a new catalog query, since every MCP
request already takes a catalog snapshot before it dispatches, `tools/list`
included. The cost is the decision itself, not the lookup.

There is precedent for this. `RESTRICT_TO_USER_OBJECTS_ALLOWED_OIDS` in
`src/sql/src/rbac.rs` already exempts `mz_show_my_cluster_privileges`, and the
comment there says it is "useful for a restricted session to inspect its own
privileges". That is the same idea these two tools are built on.

It is also a constraint to design around, and it shapes how we build both
tools. That list holds three OIDs today, the two MCP data product views and
`mz_show_my_cluster_privileges`. `mz_internal.mz_show_all_my_privileges` is the
obvious backing view for `get_permissions` and it is not on the list, so a
SQL-backed implementation would be blocked in exactly the restricted sessions
that most need the answer. The same applies to `get_settings` if it reads role
defaults from `mz_catalog.mz_role_parameters`.

So each tool has to pick one of two implementations, and the choice is part of
this design rather than an implementation detail. `get_settings` should read
effective values from session state, which needs no catalog access. For
`get_permissions`, either read the catalog snapshot the request already holds
through the privilege APIs in `src/sql/src/catalog.rs`, or add
`mz_show_all_my_privileges` to the exempt list the same way the MCP data product
views were added. Reading the snapshot is preferred, since it keeps the exempt
list small.

### How an admin controls access

Nothing here adds a way to grant or revoke a tool. Access is controlled in
three places that already exist, and the two new tools report the result
rather than set it.

**The operator, for the whole environment.** The `enable_mcp_*` dyncfgs turn
each endpoint, and each optional tool on it, on or off. They apply to
everyone, not to a role.

**The admin, per role.**
`ALTER ROLE analyst SET restrict_to_user_objects = true` confines that role to
user objects and the MCP data product views. Only a superuser can set it, and
a plain `SET` of it is refused, so a role cannot lift the restriction for
itself and neither can a query the agent writes.

**The admin, per object.** The grants they already write: `SELECT` on what the
role should read, `USAGE` on the cluster the query runs on.

So "give this analyst a narrow agent" is one `ALTER ROLE` plus the usual
grants. `get_permissions` and `get_settings` then let the agent read back what
that produced, and say why a tool call failed. They report, they do not
configure.

Two roles on the same URL, with different tools working for each:

```sql
-- Developer: system catalog only. No USAGE on any user cluster.
CREATE ROLE dev;
-- query_system_catalog works: auto_route_catalog_queries is on by default, so a
-- catalog-only read is forced onto mz_catalog_server, which grants USAGE to
-- PUBLIC. query fails: it runs SET CLUSTER, and dev has USAGE on no cluster.

-- Production app: user data only.
CREATE ROLE app;
GRANT USAGE ON CLUSTER prod TO app;
GRANT SELECT ON TABLE sales.orders TO app;
ALTER ROLE app SET restrict_to_user_objects = true;
-- query works on cluster prod. query_system_catalog fails: the restriction
-- rejects system catalog objects, and only the MCP data product views are
-- exempt.
```

Both roles see the same six tools in `tools/list`. What differs is which ones
succeed, and `get_permissions` and `get_settings` tell each agent which it has.
That is the shape the tool list cannot express, since the difference comes from
cluster USAGE and a role setting rather than from anything attached to a tool.

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
- Clients that move to `/api/mcp` see the new names and the two new tools.
- `read_data_product` is deprecated and goes away under DEX-65.

## Minimal Viable Prototype

Build `/api/mcp` behind a feature flag, serving the merged tool list, with the
two old routes pointed at the same code. Add `get_permissions` and
`get_settings`. Leave the audit table and the protocol uplift out.

Then run the admin flow end to end in a real Claude organization: add the
connector once as an admin, and connect as two users with different roles.

What we are trying to learn:

1. Does one URL plus `get_permissions` actually answer "what can this person do"
   for an admin, or do they still need to read docs?
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

**Add a privilege for MCP tools**, such as
`GRANT MCP TOOL query_system_tables TO analyst`. This would make filtering one
uniform rule, and it would extend Postgres rather than break it. Rejected because
it adds a new privilege type to the ACL system. The PRD's own comparison argues
against keeping a second permission system, and this would be one.

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
   their own identity, so this design does not depend on the answer. Tracked in
   DEX-99.
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
   away from. Tracked in DEX-99.
3. **Do the old routes really need their own tool list?** If the prototype shows
   agents handle unexpected tools well, we could point the old routes at the full
   list and drop the per-route config.
