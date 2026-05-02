# Console Architecture

The Materialize Console is a client-only react app hosted on Vercel. This
document is intended to describe the architecture at a high level and how it
fits into Materialize cloud.

## Vercel Environments

The production domain is <console.materialize.com>. Merged PRs will automatically
deploy to production.

We have a staging domain: <staging.console.materialize.com>. It points at the
staging cloud stack, but it's just a CNAME to the production Vercel deployment,
so there is never a difference in the Console code between staging and
production. Note that we do have a separate LaunchDarkly environment
configured, so feature flags can target staging and production separately.

Vercel automatically creates a preview deployment for each commit in a PR, and
these deployments are given unique subdomains. Each PR also has a subdomain
that includes the branch name that tracks the most recent commit to the PR
These domains look like `<branch|commmit>.preview.console.materialize.com`.

## Materialize database API

Materialize has several logical parts. Briefly, those are STORAGE, COMPUTE and
ADAPTER. They map to the following binaries:

- environmentd - ADAPTER
- clusterd - STORAGE and COMPUTE

Environmentd is the "frontend" of the database, it accepts connections,
manages sessions, parses SQL, maintains the catalog of SQL objects and much
more. It also exposes [HTTP](https://materialize.com/docs/integrations/http-api/)
and [websocket](https://materialize.com/docs/integrations/websocket-api/) APIs
that are used by Console.

## Cloud APIs

Materialize cloud exposes several APIs that Console uses.

- Region Controller
  - List environment assignments
  - Create environment assignment
- Environment Controller
  - List environments

When a customer enables a region (e.g. aws/us-east-1) from console, the region
controller creates an environment assignment (this is a kubernetes custom
resource definition). Technically, it's possible for a region to have multiple
environments per customer, but we don't expose this capability to users.

Listing environment assignments tells us which regions a customer has enabled,
and listing the environments gives us the environmentd hostname, which allows
us to make SQL queries on behalf of the logged in user.

## Stacks

Because we use "environment" to refer to a database, Console refers to local,
staging, production, etc as "stacks". Most local development is done by running
a local copy of Console pointing at the staging cloud stack. Console has a
stack switching UI that allows runtime switching between stacks, so you can
also point it at personal stacks (the cloud team has individual cloud stacks
for testing infrastructure changes) or even local services.

## State management

Console uses Jotai for global state management where applicable, but most
state is stored in components with the useState hook. Storing state globally
can be tricky to get right, in particular when it comes to invalidating /
cleaning up the state. With component state, we get that behavior for free
because the component gets unmounted.
