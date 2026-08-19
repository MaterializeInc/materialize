# Console Admin Panel and Auth Flows on Ory

- Associated: [DEP-181](https://linear.app/materializeinc/issue/DEP-181), [DEP-139](https://linear.app/materializeinc/issue/DEP-139), [DEP-143](https://linear.app/materializeinc/issue/DEP-143)
- Spike branch: `jasonhernandez/materialize@console-ory-converged`
- Prior art in this directory: `20240820_replace_frontegg_components.md`, `20251126_enterprise_rbac_panel.md`

## The Problem

Two separate gaps get discussed as one "admin panel" problem. They have
different owners, different urgency, and different solutions.

**1. Self-managed ships a demo container as its auth UI.** The enterprise SSO
stack deploys `kratos-selfservice-ui-node`, which is Ory's example
application for demonstrating their React libraries, not a supportable
product. A container scan flagged a large number of unpatched
vulnerabilities in it (DEP-181). It is also the reason the deployment needs
per-service hostnames (`kratos.`, `hydra.`, and others), because separate
origins were the workaround for CORS. Those hostnames each need a DNS
record, a certificate and load balancer capacity, which is the friction
currently blocking the internal dogfood deployment.

**2. Cloud account administration is an iframe we do not own.** Everything
behind "Account settings" is Frontegg's `AdminPortal`, mounted from
`src/layouts/ProfileDropdown.tsx` and `src/components/MfaAlert.tsx` via
`AdminPortal.show()`. Organizations backed by Ory have no equivalent
surface. Independently, the vendor has dropped the JWT group-claim fix from
their roadmap, which blocks SCIM and role mapping from reaching GA on Cloud,
so the pressure to have a non-Frontegg path is now real rather than
aspirational.

What the console already has is worth stating plainly, because it changes the
size of the ask:

- `src/platform/auth/Login.tsx` is already a first-party login screen. It
  does OIDC redirect through `react-oidc-context` with password fallback, and
  it already talks to Hydra on self-managed. No Frontegg involved.
- `src/platform/roles/` is already a first-party admin panel for database
  roles, privileges, users and role inheritance, driven by SQL against
  `mz_roles` and `SHOW PRIVILEGES`.

So the console is not starting from zero. The missing half is *identity*
administration: organization members, invitations, IdP connection setup,
directory sync status, and org-level tokens.

## Success Criteria

1. Self-managed deploys no third-party demo container. Every user-facing auth
   screen is served by the console image.
2. The Ory services are reachable through a single origin, so a deployment
   needs one DNS record and one certificate, and CORS configuration stops
   being a deployment variable.
3. An organization admin can complete IdP setup, member management and
   directory-sync verification without leaving the console and without
   `kubectl` or SQL.
4. Identity administration and the existing roles panel read as one product,
   not two panels that happen to share a nav bar.
5. The console runs against Frontegg or Ory selected at runtime, so Cloud can
   migrate organizations incrementally rather than in a single cutover.

## Out of Scope

- Retiring Frontegg on Cloud. This design makes that possible and does not
  schedule it.
- Billing, subscriptions and payment surfaces, which stay on their current
  path regardless of identity provider.
- The database RBAC panel itself. It exists and ships. This design only
  places it in a shared information architecture.
- Choosing a commercial identity vendor. That decision is recorded below as
  context, not reopened here.

## What Ory Provides, and What It Does Not

This is the crux of the scoping disagreement, so it is worth being precise.
The following is read off Ory's published source rather than marketing pages.

`@ory/elements-react` exports exactly seven themed flows from
`theme/default/flows`: **error, login, recovery, registration, settings,
verification, and consent**. Its component directory contains exactly four
groups: `card`, `form`, `generic`, `settings`. That is the whole surface, and
it is entirely the end-user self-service half.

It does **not** provide organization or member management, role assignment,
IdP connection configuration, directory-sync status, audit log, or MFA policy
administration. There is no `@ory/elements` equivalent of Frontegg's
`AdminPortal`, and no amount of configuration produces one.

Two package facts that affect planning:

- Latest is **1.2.1**, published 2026-07-31. The spike pins 1.1.0, so it is
  one minor behind before the work even starts.
- Peer dependencies are React 18 or 19. The console is on React 18.3.1, so
  there is no framework upgrade hiding in this.
- The package publishes a `./theme/tailwind` export alongside
  `./theme/styles.css`, confirming the theme is Tailwind-coupled rather than
  incidentally styled.

For enterprise connection setup specifically, **Ory Polis** ships two things:
a standalone Next.js admin portal, and an npm library. The library is a
**Node backend library**: its entry point requires `externalUrl`, `samlPath`
and `acsUrl`, and it exposes server-side controllers
(`ConnectionAPIController`, `AdminController`, `initDirectorySync`,
`SetupLinkController`, `OAuthController`, `BrandingController`). The console
is a client-only Vite SPA with no Node server of its own, so **the library is
not embeddable in the console**. The console's only route to Polis is its
HTTP API.

Note `SetupLinkController`: Polis has a first-class notion of a setup link,
a scoped URL you hand to a customer's IT admin so they configure their own
connection without an account in our console. That is the same primitive as
the WorkOS portal link discussed below, and it is a cheaper answer to a large
part of Phase 3 than building the full guided flow ourselves.

The conclusion that follows: **Elements replaces the login and account
screens almost for free. The admin panel is ours to build, with the single
exception of IdP connection setup, where Polis gives us a choice between
proxying its portal, driving its HTTP API, or handing out its setup links.**

## Solution Proposal

Five phases. Phases 0 and 1 remove the security and deployment blockers and
are worth doing on their own merits even if everything after them is
deferred. Phases 2 onward are the actual admin panel.

### Phase 0: Collapse Ory onto the console origin

The console container already proxies `/api` to environmentd through
`console/misc/docker/nginx.conf.template`. Extend the same template with
`/ory/kratos`, `/ory/hydra` and `/ory/polis` locations pointing at the
in-cluster services.

This is the smallest change with the largest effect. It gives one origin, so
Kratos session cookies and the Hydra login and consent round trip stay
same-site, and CORS configuration stops being something a customer can get
wrong. It removes the per-service hostnames that are blocking the dogfood
deployment.

The one coordination hazard: Hydra's issuer URL is part of its OIDC
discovery document and is validated by environmentd and by the console's
`MzOidcUserManager`, which reads `oidc_issuer` from `/api/console/config`.
Moving Hydra to a path prefix changes the issuer string, so `urls.self.issuer`
in the Hydra config, the `oidc_issuer` system parameter, and every registered
redirect URI have to move together. Polis is the exception that cannot hide
behind the console origin: the customer's IdP is the SCIM client and needs a
route to it, so its endpoint stays separately routable and needs to be called
out in the docs.

Terraform already has the enabling change in flight, making the standalone
selfservice UI optional in the module.

*Owner: deployment, with console reviewing the proxy config. Estimate: small,
around a week, dominated by the issuer migration rather than the nginx edit.*

### Phase 1: Serve the Ory self-service flows from the console

Add `@ory/elements-react` behind a facade at
`src/external-library-wrappers/ory-elements.ts`, matching the existing
`frontegg.ts` and `oidc.ts` pattern and the `no-restricted-imports` ESLint
rule that enforces it. Mount the flow pages under `/auth/*`, wire them to the
proxied Kratos and Hydra endpoints from Phase 0, and delete the demo
container from the Terraform module.

The spike branch has already done most of this and should be the starting
point rather than a reference. It carries `@ory/elements-react` at 1.1.0
(bump to 1.2.1 as step one) plus
`@ory/client` and `@ory/client-fetch`, the facade file, the login,
registration, recovery, verification, settings and callback pages, and a
Playwright suite that runs against a real Ory provider. It targets Cloud and
Ory Network, so the flow pages port over but the configuration layer in
`src/config/oryUrls.ts` does not: it resolves Ory Network project URLs per
stack, where self-managed needs the issuer from
`/api/console/config` exactly as `oidc.ts` already does.

Two risks worth pricing in now rather than discovering later:

- **Styling.** Elements ships a Tailwind-based theme. The console is Chakra.
  Importing `@ory/elements-react/theme/styles.css` wholesale brings Tailwind
  preflight into a Chakra application and will fight it. The mitigation is to
  use the component override hooks (`OryFlowComponentOverrides`) with our own
  Chakra primitives and skip the bundled theme, which costs more up front and
  produces screens that match the rest of the console. Decide this
  deliberately, because retrofitting it later means rewriting the flow pages.
- **Dark mode.** The console has a theme switcher. Whichever styling route we
  take has to follow it, and the bundled theme will not do so on its own.

*Owner: console. Estimate: two to three weeks with the spike as the base, of
which styling is the majority.*

### Phase 2: Admin panel shell and member management

Introduce a single `/admin` section with the existing roles panel moved under
it, so identity and database administration share one information
architecture. Roughly:

```
/admin/members     org members, invitations, deactivation   (new, identity)
/admin/roles       existing platform/roles, unmoved          (exists)
/admin/access      app passwords and service accounts        (partly exists)
/admin/sso         IdP connection and directory sync         (Phase 3)
```

Member management is genuinely new work: a members list, an invite flow, an
invite acceptance page, and deactivation. The spike has all four for Cloud
against the cloud global API. Self-managed has no equivalent API, and this is
the significant open question in this design. Kratos owns identities;
whether the console reads members from Kratos's admin API, from SCIM state
in Polis, or from `mz_roles` where they already surface after group sync, is
an unresolved backend question and needs a decision before this phase can be
estimated with confidence.

Access management is the other half. App passwords on self-managed will be
backed by Talos, which is deployed but not yet hooked up (DEP-143), with an
experimental authenticator open as a draft PR. The console has an app
passwords page today and the spike has an Ory-backed variant.

*Owner: console, blocked on a members-source decision. Estimate: three to
four weeks once unblocked.*

### Phase 3: IdP connection and directory sync setup

This is the surface that matters most to the buyer and the one the team has
the least of. It is a guided flow: pick an IdP, get the callback URL, ACS URL
and entity ID to paste into it, upload or point at metadata, map the group
claim, then verify that directory sync is actually delivering users.

Four options, in increasing cost. Embedding the Polis npm library is not
among them, for the reason given above: it is a Node backend library and the
console has no server.

1. **Hand out Polis setup links.** Use `SetupLinkController` through the
   Polis API to mint a scoped link, and have the console do nothing but
   generate, display and revoke it. The customer's IT admin configures the
   connection in Polis's own screens. Cheapest by a wide margin, and it
   matches how the buyer actually works, since the person who administers the
   IdP is frequently not the person who deploys Materialize. Worth shipping
   first even if we later build option 3.
2. **Proxy the Polis Admin Portal under the console origin.** Reuses the
   Phase 0 proxy so it is at least same-origin and single-certificate, but it
   is still a second image to patch and a second design language.
3. **Build against the Polis HTTP API.** Connection management under
   `/api/v1/sso`, directory sync under `/api/v1/dsync`. We render our own
   screens and own the long-tail IdP guidance as our own content. Best end
   state, highest cost.
4. **Reimplement connection storage ourselves.** Not justified.

Recommendation is to ship option 1, then decide between 2 and 3 with real
usage data. Option 3 is what eventually makes group-claim mapping legible,
which has been the hardest part of the current setup to explain.

*Owner: console with security. Estimate: about a week for option 1, four to
six weeks for option 3.*

### Phase 4: Runtime provider selection and Cloud convergence

Generalize `AppConfigSwitch` so `authMode` selects the provider at runtime,
and lift the shared pieces into a module both deployments consume. The spike
already contains `detectAuthProvider.ts`, which resolves the provider before
authentication happens by checking, in order, a URL parameter, the issuer of
an existing session, a sticky localStorage preference, and a server-side
email-domain lookup. That ordering exists to solve a real chicken-and-egg
problem: feature flags need a session, and the session needs a provider.

Only after this phase is retiring the Frontegg iframe on Cloud a scheduling
question rather than an engineering one.

*Owner: console. Estimate: two to three weeks, plus migration work outside
the console.*

## Where WorkOS Fits

WorkOS keeps coming up and should be recorded rather than relitigated each
time.

It was evaluated in November 2025. The finding was that it is entirely cloud
hosted and requires network egress, which rules it out for self-managed and
air-gapped deployments without changing the product model. Its admin portal
and setup guidance were rated highly on UX. A test account exists and access
has been extended to more of the team, and the current position from security
is that Ory makes the most long-term sense across the stack while remaining
open to alternatives for Cloud or BYOC specifically.

Mechanically, the WorkOS Admin Portal is reached through a **portal link**
generated per organization from their API, scoped by an intent. The published
intents are `sso`, `dsync`, `audit_logs`, `log_streams`,
`domain_verification`, `certificate_renewal` and `bring_your_own_key`, and a
generated link stays valid for around 30 days. The console's entire
integration would be one API call and a redirect.

That is worth noticing, because it is structurally the same shape as Polis
setup links. Both replace a guided connection-setup UI with a minted,
scoped URL. The difference is where the screens are hosted and whether egress
is required, not how much console code is involved.

For this design that means:

- **Self-managed: not an option.** Hosted-only and egress-dependent. No
  console work, now or later.
- **Cloud or BYOC: still live.** Adopting it for connection setup would
  collapse Phase 3 to a portal-link call, at the cost of the same
  embedded-third-party-surface pattern we are trying to leave, plus a hard
  egress dependency.

The load-bearing consequence: **Phase 3 is the only phase whose design
changes if WorkOS is chosen for Cloud.** Phases 0 through 2 and Phase 4 are
provider-shaped work that we need regardless. That is a good reason to
sequence Phase 3 last among the build phases and to keep connection setup
behind an interface rather than wiring Polis calls directly into components.

## Minimal Viable Prototype

The prototype exists and does not need to be rebuilt. The spike branch is
roughly 7,600 lines across 61 files and demonstrates Ory Elements flows,
members, invites and app passwords running inside this console against a real
provider. It was written as an exploration rather than for review, so it
needs a hardening pass, but it answers the "can Elements live inside a Vite
and Chakra SPA" question empirically, which was the main technical unknown.

The gap worth prototyping next is Phase 0, since the issuer migration is the
step most likely to produce surprises, and it unblocks the dogfood deployment
that is currently stalled.

## Alternatives

**Keep the demo container and harden it.** Rebuild it from a supported base
image and track upstream. Cheapest immediately, but it keeps the multiple
origins, keeps a second frontend in a second framework, and leaves the
console with no path to a first-party admin surface. It is a reasonable
bridge if Phase 1 slips, not a destination.

**Build the identity admin panel against Kratos and Hydra admin APIs
directly, skipping Elements.** More control, no styling conflict, and
considerably more code in exactly the area where the team has said it does
not want to own code. The stated principle is buy over build for security
surfaces where the vendor tests more rigorously than we would, and every line
of auth code carries an ongoing audit cost. Using Elements for the flows and
building only the admin surface splits that correctly.

**Ship the panel only on Cloud and leave self-managed on the demo
container.** Inverts the urgency. Self-managed is where the container is
unpatched and where the deployment is blocked today.

## How the Vendor Claims Here Were Checked

Ory's and WorkOS's documentation sites are unreachable from the environment
this was drafted in, so nothing here rests on a marketing page. The vendor
claims come from primary sources instead:

- Flow and component lists: the export files in `ory/elements` at
  `packages/elements-react/src/theme/default/flows/index.ts` and
  `packages/elements-react/src/components/index.ts`.
- Package version, peer dependencies and the Tailwind theme export: the npm
  registry metadata for `@ory/elements-react`.
- Polis architecture, the backend-library constraint and the controller list
  including setup links: `ory/polis` at `npm/src/index.ts`, plus its README.
- WorkOS portal links and intents: WorkOS SDK and API reference material
  reached through search rather than their docs site.

Anything about commercial terms, support scope or roadmap is deliberately
absent, since none of it could be verified from these sources.

## Open Questions

1. **Where do self-managed organization members come from?** Kratos admin
   API, Polis SCIM state, or `mz_roles` after group sync. Phase 2 cannot be
   estimated until this is answered, and it is a backend decision, not a
   console one.
2. **Bundled Elements theme or Chakra component overrides?** Cheap now,
   expensive to reverse. Recommendation is overrides.
3. **Does Phase 3 target Polis, or does a Cloud WorkOS decision change it?**
   Sequencing Phase 3 last buys time to answer this without blocking anything
   else.
4. **Who owns the identity admin panel?** The work has been described as
   explicitly out of scope for the current SSO project while simultaneously
   being the thing blocking a production-worthy deployment. Phases 0 and 1
   are small enough to fund immediately. Phases 2 and 3 are a quarter-scale
   commitment and need to be planned as such rather than absorbed.
5. **Does the roles panel move under `/admin`?** It is shipped and has users,
   so the move is a URL change with redirects, but it should be a deliberate
   product decision with design input rather than a side effect.
