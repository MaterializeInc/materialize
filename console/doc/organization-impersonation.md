# Organization Impersonation

Organization impersonation is a feature that allows materialize employees to
access customer environments through the console, with authorization provided
by Teleport.

## User flow

Users will generally use this feature by going to the org lookup dashboard in
Grafana, which includes a link to the per environment console url (e.g.
https://global-api-aws-us-east-1-staging.materialize.teleport.sh/redirect?to=https%3A%2F%2Faws-us-east-1-xqkye4k37je3tpo4q3qunp6bza-0.materialize.teleport.sh%2Finternal-console%2F%23staging
).
Unless you already have an active Teleport session, you will need to log into
Teleport with Okta, which will then redirect you back to the original URL,
which is the Teleport proxy to the Global API, which then redirects you to the
Teleport proxy for the target environmentd's internal http endpoint. At this
point, you have access to all console features that are
powered by catalog / internal tables and Global API. The mz_support user does
not have permission to customer objects, so you can't for example go into the
shell and select from their views.

## Technical details

The internal environmentd interfaces are very similar to the public ones,
except they don't require any authentication, because they are not exposed
outside of the k8's network and Teleport. When coming in through Teleport, the
X-Materialize-User header is injected by Teleport with the value `mz_support`.
This means the user will have read-only access to the customer environment. The
`/internal-console` route reverse proxies to
`internal.console.materialize.com`, which is a Vercel deployment that builds
console with the basename `/internal-console`, so that routing works correctly.
This reverse proxy is required because Teleport blocks CORS preflight requests
(since preflight requests cannot have cookies, and teleport rejects any
requests without a cookie). Console has logic in the config module that
detects the `*.materialize.teleport.sh` hostname and extracts the region, org
ID and environment ordinal from the subdomain. In this mode, we disable any
features that use frontegg data or non-global cloud APIs. Calls to environmentd
are made against the teleport hostname, rather than the normal public
environmentd hostname.

By proxying users through the Cloud global API, which has a redirect endpoint,
the user session can get a Teleport cookie for that origin as well, which makes
certain requests to the Cloud global API possible. To take advantage of this,
requests must be simple and non-mutating. To achieve auth in this Frontegg-less
environment, we overload the `Accept` header to include the organization ID.

## Development

To enable impersonation for local development, you will first need to open a
Teleport proxy using `tsh`.

```shell
tsh proxy app aws-us-east-1-w2xt5l6tyneptac4ugfrtuflsu-0 --port 6876
```

Then start the console server with `IMPERSONATION_HOSTNAME` set.

```shell
IMPERSONATION_HOSTNAME=aws-us-east-1-w2xt5l6tyneptac4ugfrtuflsu-0.materialize.teleport.sh yarn start
```

Local impersonation uses port 6878 as the local loop back address.
