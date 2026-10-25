# Formal methods for SQL RBAC

- Associated:
  - Branch: `claude/sql-rbac-formal-methods-79oiky`

## The Problem

RBAC is security critical and verified almost entirely by example. The policy is
a single `match` in `generate_rbac_requirements` (`src/sql/src/rbac.rs`), one
arm per `Plan` variant, about 1,200 lines. There is no specification apart from
that code. As a result:

1. **The policy cannot be reviewed as a policy.** Answering "which statements
   create objects in a schema" means reading every arm.
2. **An omission looks like a decision.** An arm returning `Default::default()`
   may mean "needs nothing" or "nobody thought about it".
3. **Authorization questions are answered in more than one place.** The Rust
   check, the SQL `has_*_privilege` functions, `mz_session_role_memberships()`,
   the `mz_show_*_privileges` views, catalog-table visibility, and
   pre-execution validators on some endpoints each answer a version of "may
   this role see or do this". Their agreement is asserted in comments, not
   checked.
4. **Nothing forces enforcement.** `check_plan` is called from the sequencer and
   the frontend peek path, and `check_usage` separately before purification.
   A new path that skips them is a silent bypass.

"Prove RBAC correct" needs splitting. Consistency properties are checkable
cheaply. Intent ("should this statement need `CREATE`?") is not checkable by any
tool. This document separates the two and proposes an order of work.

## Success Criteria

- The policy exists as a reviewable artifact, and a change to it shows up as a
  diff in the PR that causes it.
- Cross-cutting shape properties of the policy are checked in CI.
- "This path performs an authorization check" is a compile-time property.
- Agreement between independent answers to the same authorization question is
  tested.
- Residual unverified risk is written down.

## Out of Scope

- **Information flow in the query engine.** Leaks through error messages,
  timing, introspection relations, or optimizer output are not properties of
  `rbac.rs`. RBAC is one conjunct of "a role cannot observe what it was not
  granted".
- **Policy intent.** Formal methods check that decisions are applied
  consistently, not that they are the right decisions.
- **Authentication.** Who the session's roles are is taken as given. Attributes
  that confer authority, such as superuser, are not. They are catalog facts and
  are in scope (see P12).

## The decision procedure

`check_plan` computes:

```
req  = gen(C, plan, current_role)      -- generate_rbac_requirements
req' = filter(C, s, req)               -- filter_requirements
accept iff validate(C, s, req')        -- RbacRequirements::validate
```

`gen` returns required role memberships, object ownerships,
`(object, mode, role)` privilege triples, item types needing `USAGE`, and an
optional superuser-only marker. `filter` drops to the mandatory part (system
objects only, `SELECT` removed, no superuser marker) when the session is a
superuser or has RBAC disabled. `validate` checks:

```
U: every resolved item of a listed type has USAGE on it and its schema
M: required memberships are held by current_role
O: every required object is owned by a role current_role holds
P: every (o, m, r) has m in eff(r, o)   (temporary schemas skipped)
```

where `holds(r)` is the membership closure plus `PUBLIC`, and
`eff(r, o)` is the union of `o`'s privileges over `holds(r)`. `P` carries its
own role so reads through a view are attributed to the view's owner.

Two guarantees already hold. The `match` has no wildcard arm, so a new `Plan`
variant fails to compile until it gets a policy. `Op::GrantRole` rejects cyclic
membership using the full closure.

## Properties

**Class A, the decision procedure alone.**

- **P1 Totality.** `check_plan` never panics. It currently calls panicking
  getters, including on the denial-formatting path.
- **P2 Grant monotonicity.** Adding privileges or memberships never turns an
  accept into a deny.
- **P3 Relaxation soundness.** `filter` only weakens.
- **P4 Closure.** `collect_role_membership` computes `holds`, terminates on
  cycles, and always includes `PUBLIC`.
- **P5 Uniformity.** Shape properties of `gen` across all plan variants:
  - a plan that creates an object in a schema requires `CREATE` on that schema,
    whichever statement family creates it, including `ALTER`.
  - a plan that places work or objects on a cluster requires a privilege on
    that cluster.
  - a plan whose output depends on stored data is a read, whether or not it
    returns rows, and requires what a `SELECT` of that data requires.
  - a plan that mutates or drops an object requires ownership or a named
    privilege on it.
  - no plan requires a privilege on an object it does not reference.
- **P10 Delegation.** View-owner attribution matches PostgreSQL and loses no
  requirement when an item is reached by two roles.
- **P13 Owner privileges.** Every object's ACL contains its owner's privileges,
  preserved by `ALTER OWNER`, `DROP OWNED`, and `REVOKE`.

**Class B, agreement between implementations.**

- **P6 Predicate agreement.** The Rust decision agrees with `has_*_privilege`,
  `mz_session_role_memberships()`, the `mz_show_*_privileges` views, and
  catalog-relation visibility. A view that advertises access enforcement then
  denies is worse than the reverse, because agents act on those views.
- **P8 Path agreement.** The sequencer and frontend peek paths assemble
  `check_plan`'s arguments independently, including `target_cluster_id`. They
  must reach the same decision.
- **P11 Effect ordering.** Purification contacts external systems before a plan
  exists. What holds is that it is preceded by `check_usage` on the pre-plan
  `resolved_ids`, provided purification only touches those items. The proviso
  is unverified.
- **P14 Validator agreement.** Any check that inspects a statement before name
  resolution must decide on resolved identities, not on names as written.

**Class C, beyond this work.**

- **P7 Chokepoint.** Every executed plan was authorized. A call-graph property.
- **P9 Read-set soundness.** `resolved_ids` over-approximates everything a
  statement reads or creates. `U`, delegation, and `restrict_to_user_objects`
  all rest on it. A plan builder that returns an empty or partial set silently
  weakens all three.
- **P12 Staleness.** Authority binds at statement start. What is the bound, and
  does it cover authority-conferring session attributes as well as grants?

## Solution Proposal

Ordered by value per cost. The instinct is to start with a model checker. The
failure modes we have (an arm that misses a requirement, a path that skips the
check, two answers drifting apart) are invisible to a model disconnected from
the policy table, so start with the table.

### Layer 0: write the policy down, make the chokepoint a type

**0a. Golden dump.** A datadriven test plans SQL against `Catalog::with_debug`
and prints the requirement record via a public
`describe_rbac_requirements`. The harness must pass a real session role and a
real target cluster, or the cluster dimension of P5 is invisible.

**0b. P5 over the dump.** Assert the P5 clauses over every dumped record. This
needs a per-plan classification of effects (creates in schema, uses cluster,
reads data). That classification is hand-maintained and can itself be wrong,
but it is small and reviewable, which the `match` is not.

**0c. `Authorized<Plan>`.** Sequencer entry points accept a token only
`check_plan` can mint. Discharges P7 by construction. A capability minted by
`check_usage` and required by purification's accessors does the same for P11.

### Layer 1: executable specification

Write `holds`, `eff`, and `U`/`M`/`O`/`P` a second time as a small pure model
over synthetic state, implement the RBAC-relevant subset of `SessionCatalog`
over it (`src/mz-deploy` has two partial implementations to crib from), and
check `rbac.rs` against it with `proptest`. Covers P1 to P5, P10, P13. The model
must be written from intended semantics, not by sharing code with `rbac.rs`, or
it cross-checks nothing.

### Layer 2: bounded model checking on integer kernels

Kani is in the tree (`src/ore/src/pool/region.rs`). Its own performance tests
show that a nondeterministic `BTreeSet` of one element is already expensive, so
a role graph as `BTreeMap<RoleId, BTreeSet<RoleId>>` is out of reach. Give Kani
integer encodings instead: `AclMode` is already a bitflags integer, and the role
graph becomes adjacency bitmasks, which makes P4 provable at 64 roles. Check the
encoder against the real collection code with `proptest`. Contracts, loop
contracts, and autoharness are experimental, and loop contracts do not support
`while let`.

### Layer 3: temporal model for P12 only

A TLA+ spec of the catalog transaction boundary, per-statement snapshots,
long-lived `SUBSCRIBE`, and session attribute caching. Do not model the policy
table.

### Differential work (P6, P8, P14)

- P6: generated sqllogictest comparing `has_*_privilege` and catalog visibility
  against attempting the statement.
- P8: under `ci` debug assertions, assert both paths produce equal requirement
  sets. Bind the sets outside the `debug_assert!`.
- P14: route pre-execution validators through the resolver.

### Refactors that gate Layers 1 to 3

1. **Split gather, decide, diagnose.** `gather` reads the catalog, `decide` is
   pure data in and out, `diagnose` formats errors and runs only on denial.
   `decide` becomes monomorphic, which every tool requires. Missing facts must
   deny: an omission in `gather` then causes a spurious denial, never a bypass.
   Authority-conferring attributes such as superuser are gathered like any
   other fact.
2. **Policy combinators.** Keep the exhaustive `match`, but build arms from a
   small vocabulary (`creates_in_schema`, `creates_on_cluster`, `reads`) so P5
   holds by construction, and replace bare `Default::default()` with a named
   "requires nothing" constructor.
3. **One membership closure.** Project `session_role_memberships` from
   `collect_role_membership`. They differ today on `PUBLIC`, compensated at call
   sites.

### Residual risk

P9 stays an assumption until a runtime check asserts that every id touched in
planning is in `resolved_ids`. That is the most valuable follow-on. Policy
intent and information flow remain unverified.

## Tool selection

| Tool | Guarantee | Cost | Precondition | Use for |
| --- | --- | --- | --- | --- |
| `proptest` | random search | low | none | P1 to P5, P10, P13 |
| Kani | bounded proof | low | monomorphic, integer data | P1, `AclMode`, P3, P4 |
| Verus / Creusot | unbounded proof | high | language subset | P2 to P4 as theorems |
| Aeneas + Lean | unbounded proof | highest | no trait bounds or objects | Rust-to-SQL equivalence (P6) |

None apply to `check_plan` as written, which is generic over `impl
SessionCatalog` and consumes trait objects. The question is the refactor first,
then the tool. Lean is the only option that can state P6 as a theorem, since one
side is SQL. Defer it until the differential test keeps finding divergences.

## Minimal Viable Prototype

On this branch:

1. `RbacRequirementsDescription` and `describe_rbac_requirements` in
   `src/sql/src/rbac.rs`.
2. A golden dump in `src/adapter/tests/rbac.rs` for the `CREATE` family and the
   read path.
3. A `proptest` for P3.
4. An integer membership kernel in `src/sql/src/rbac/kernel.rs`, checked against
   a set-based reference.

Known gaps. The dump uses a role absent from the catalog and no target cluster.
The kernel's reference omits `PUBLIC`, and neither it nor the kernel is the
production closure, so P4 is not yet checked against running code. Next steps
are 0a's harness fixes, then 0b.

## Alternatives

- **Verify `rbac.rs` as is.** Not possible with any surveyed tool.
- **Verify a copy, leave `rbac.rs` alone.** The copy diverges and is not what
  runs.
- **TLA+ or Alloy first.** Blind to the failure modes we have. Useful later for
  P12.
- **A declarative table as the specification.** Improves readability, but a
  single implementation cross-checks nothing.
- **Fuzz RBAC in parallel-workload.** Finds P1 in situ, has no oracle for
  P2 to P5. Complementary.
- **Differential testing against PostgreSQL.** Right for the pg-compatible
  surface, silent on clusters, connections, secrets, and `mz_support`, which is
  where our policy has no reference to copy.
- **More sqllogictests only.** The baseline. Cannot state a cross-cutting
  property.

## Open questions

1. Who owns the Layer 1 model? It survives only if updating it is part of
   landing an RBAC change.
2. What does an eager `AuthzFacts` cost on the frontend peek path?
3. Should `describe_rbac_requirements` stay public for operator introspection,
   or be test-only?
4. Is post-`REVOKE` behavior of in-flight `SUBSCRIBE` a documented commitment?
5. Is the P8 shadow check CI-only or permanent?
