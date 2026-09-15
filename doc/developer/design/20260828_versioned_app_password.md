# Versioned app password format

- Associated:
  - [CLO-218: Add a versioned app password format that encodes tenant info for balancerd](https://linear.app/materializeinc/issue/CLO-218)
  - [CLO-225: Design the versioned app password format](https://linear.app/materializeinc/issue/CLO-225)
  - `src/frontegg-auth/src/app_password.rs`, `src/balancerd/src/lib.rs`

## The Problem

A Materialize app password is an opaque `mzp_`-prefixed token that encodes two
UUIDs: a Frontegg client ID and a secret key. It carries no tenant information.

`balancerd` routes a pgwire connection by substituting a tenant UUID into
`addr_template`. On the SNI path the client supplies an environment label in the
TLS server name, `balancerd` resolves it, and recovers the tenant from the CNAME
target, so routing costs nothing beyond a DNS lookup it would do anyway. On the
non-SNI path there is nothing to route on, so `BalancerResolver::MultiTenant`
performs a full Frontegg app-password exchange purely to learn the tenant ID: it
prompts for the password, POSTs it to the Frontegg admin API token endpoint,
verifies the returned JWT, and reads the `tenantId` claim. The credentials are
then forwarded to `environmentd`, which authenticates them again.

`balancerd` runs the same `Authenticator` as `environmentd`, so this is one
Frontegg round trip per cold (password, `balancerd` pod) pair rather than one
per connection. That is still the wrong shape for two reasons. `balancerd` is a
horizontally scaled stateless fleet, so a password's first connection to each
pod pays the round trip and a scale-up or a rolling restart resets every cache
at once. More seriously, it is a hard availability dependency on a third party
sitting on the connection path: while Frontegg is unreachable or slow, every
non-SNI pgwire connection whose session is not already cached is unreachable or
slow, and cached sessions expire on the token refresh cadence during the
outage.

The fix is for the password to carry the routing data itself. That requires
changing the token format, and app passwords are long-lived credentials held by
customers that we cannot rewrite, so the format needs an explicit version scheme
in which old and new tokens coexist indefinitely.

## Success Criteria

- `balancerd` can route a non-SNI pgwire connection using only bytes the client
  already sent, with no network call, for passwords minted after the change.
- Every app password ever minted keeps working, forever, with no customer
  action. Old tokens are not a deprecated path on a sunset clock.
- The new parser never confuses the two directions: a legacy token never lands
  in the versioned branch, and a versioned token never lands in a legacy
  branch. This must hold for every legacy token, not merely for almost all of
  them.
- Code running the previous `mz-frontegg-auth` parser rejects a new token
  outright rather than silently decoding it into a wrong client ID and secret.
- A parser that meets a version it does not know fails with an actionable error
  naming the version, rather than guessing.
- Nothing that is secret today becomes readable, and nothing a client can assert
  in the token can grant access it would not otherwise have.
- The token stays a single word with no characters that need escaping in a
  connection string, environment variable, or YAML value, and it is no worse to
  select and copy than today's tokens are.

## Out of Scope

- Changing what an app password authenticates. The client ID and secret keep
  their current meaning and the Frontegg exchange is unchanged.
- The self-managed password path (`mz_auth::password`) and the OIDC path.
  Neither parses `mzp_` tokens. `mzp_` is a Frontegg/Cloud concept only.
- Rotating, re-issuing, or migrating existing passwords.
- Making the embedded tenant authoritative for anything. It is a routing hint.
  Authorization stays with the Frontegg JWT.
- Removing the Frontegg exchange from `balancerd` altogether. `balancerd` still
  falls back to it for legacy tokens, which will be the majority of the fleet
  for a long time. That fallback is CLO-227's scope.

## Solution Proposal

### Summary

Introduce format version 1:

```
mzp_v1_<base64url-nopad(48 bytes)>
```

48 bytes, laid out as three raw 16-byte UUIDs:

| offset | length | field        |
| ------ | ------ | ------------ |
| 0      | 16     | `tenant_id`  |
| 16     | 16     | `client_id`  |
| 32     | 16     | `secret_key` |

The full token is a fixed 71 characters, for example:

```
mzp_v1_qKZ0tXVFQuGgKcuFshhOI3zjwejqhUWUrV14XxfRc28ZR_3O9UBK24Skc0fl0wyf
```

which decodes to tenant `a8a674b5-7545-42e1-a029-cb85b2184e23`, client
`7ce3c1e8-ea85-4594-ad5d-785f17d1736f`, secret
`1947fdce-f540-4adb-84a4-7347e5d30c9f`.

Existing tokens are retroactively version 0. They are not reformatted, not
re-minted, and not deprecated.

### Why the version marker looks like that

The marker has to survive two parsers: the one we are writing, and the one
already deployed in the field.

Keeping the `mzp_` prefix is deliberate. Unrelated code greps for it to redact
credentials from logs and command lines, including `FILTERED_ARGS` in
`misc/python/materialize/util.py`, the mzcompose CI command hook's `sed` over
`ps aux`, and testdrive's connection-URL redaction. A new top-level prefix such
as `mzp1_` would silently disable all of that and start leaking new-format
credentials into CI logs. The version goes inside the body, where the redactors
do not look.

Within the body, `v1_` is chosen so that the previous parser rejects it
deterministically rather than probabilistically. That parser branches on length:

- Exactly 43 or 44 characters: decode as URL-safe base64 and split the 32 bytes
  into two UUIDs.
- At least 64 characters: drop every non-alphanumeric character, then parse the
  first 32 remaining characters as a hex UUID and the next 32 as another.
- Otherwise: error.

A v1 body is 67 characters, so it takes the hex branch. `_` is not alphanumeric
and is dropped, leaving a string starting `v1`. `v` is not a hex digit, so
`Uuid::parse_str` fails on the first character it inspects. Every future version
inherits this for free, because every versioned body starts with `v`.

### The 43/44 invariant, in both directions

The base64 branch accepts anything of length 43 or 44 in the URL-safe alphabet,
and `v`, digits, and `_` are all in that alphabet. That single fact creates a
hazard in each direction, and one rule closes both:

> **A versioned app password body must never be exactly 43 or 44 characters
> long, and a parser must classify a 43- or 44-character body as legacy before
> it looks for a version marker.**

Going forward, a v1 body of 43 or 44 characters would be base64-decodable by the
previous parser, which would split it into two garbage UUIDs and hand them to
Frontegg rather than erroring. v1 avoids this at 67 characters. Any future
version must too. Between the leading `v` and the length rule, code running the
previous parser rejects every versioned token that will ever exist.

Going backward, a legacy base64 body is 43 essentially random characters from
that alphabet, so roughly one in 23,000 of them begins `v0_` through `v9_` or a
two-digit equivalent. Measured over two million synthetic legacy tokens: one in
25,000. A parser that tests for the version marker first would claim those
tokens for the versioned branch, find a 40-character payload where it wanted 64,
and reject a credential that has worked for years. Ordering the length check
first makes legacy tokens win the tie unconditionally, which is the correct
precedence: legacy tokens exist and cannot be re-minted, whereas the versioned
format is ours to constrain.

Choosing 48 payload bytes also removes a footgun the legacy format has. 48 is
divisible by 3, so base64 never emits a `=` pad character. The legacy format's
32 bytes are not, which is why the parser accepts both 43 and 44 characters and
configures the decoder as padding-indifferent. v1 has exactly one valid encoding
of a given payload, and `=` never appears in a token.

### Why those fields, and no others

`tenant_id` is what `balancerd` substitutes into `addr_template`, and it is the
whole point of the exercise. `client_id` and `secret_key` have to stay: they are
the credential.

We deliberately do not embed a region or environment hint.

`balancerd` is deployed per region and its `addr_template` is region-scoped, so
a region field tells it nothing it does not already know about itself. Its only
use would be detecting that a client aimed a password at the wrong region, which
today already fails, just at DNS resolution instead of at parse time. Against
that, a region field is a second thing that can go stale relative to reality,
since regions get added and environments move. It must be encoded as a
variable-length string or a registry of magic numbers, and it would be baked
immutably into a credential we cannot rewrite. The cost outlives the benefit. If
a concrete need appears, it is a v2 field.

Likewise there is no environment generation index. `addr_template` resolution
already handles the generation, and pinning a generation into a long-lived
credential would break the tenant the first time their environment is
regenerated.

There is no checksum. Its only real value would be typo detection, and it does
not help integrity: the hint is unauthenticated regardless, so an attacker who
wants to forge a routing hint simply computes the checksum too. The fixed
`mzp_v1_` prefix plus fixed 71-character length is already a low-false-positive
signature for secret scanners, which is the other thing checksums are usually
bought for.

### Field ordering

The secret goes last, on purpose. Partial disclosures of credentials are
prefix-shaped: a truncated log line, a screenshot cut off at the edge, a
progress bar overwriting the tail. Ordering the payload
`tenant_id, client_id, secret_key` means a prefix leak gives up the fields in
increasing order of sensitivity, and the secret is the last thing to go.

### Parsing contract

```rust
pub struct AppPassword {
    pub version: AppPasswordVersion,
    pub client_id: Uuid,
    pub secret_key: Uuid,
}

pub enum AppPasswordVersion {
    /// A legacy password. Carries no routing data.
    V0,
    /// Carries a client-asserted tenant routing hint. Unauthenticated: usable
    /// for routing only, never for authorization, audit, or billing.
    V1 { tenant_id: Uuid },
}
```

Fields common to every version stay on the struct, so the many call sites that
only read `client_id` and `secret_key` are unchanged. Version-specific data
lives in the variant, so v2 is an added variant and the compiler enumerates
everything that must consider it. `balancerd` matches on `version`. Everyone
else ignores it.

Adding a field is not free at construction sites. `src/mz/src/server.rs`,
`src/frontegg-client/src/client/app_password.rs`, `src/mz/tests/local.rs`, and
the rustdoc examples in `src/frontegg-client` and `src/cloud-api` all build
`AppPassword` with a struct literal and will fail to compile until updated.
`AppPassword` also derives `Deserialize`, so `version` needs a default or those
payloads need the field. These are compile errors rather than silent breakage,
which is the point of putting the version in the type.

`from_str` resolves the version before it looks at the payload, but resolves
length first:

1. Strip `mzp_`. A missing prefix is an error.
2. If the remainder is exactly 43 or 44 characters, it is legacy. Decode it as
   base64 and yield `V0`. This step comes first for the reason given above.
3. Otherwise, if the remainder matches `v<digits>_`, it is versioned. Parse the
   digits. A known version decodes per its layout, and the payload length must
   match exactly. An **unknown version is an error naming the version**, never a
   fallthrough to a legacy branch.
4. Otherwise apply the remaining legacy heuristic, the hex form, yielding `V0`.

The error type gains a variant so the unsupported-version case can say something
useful, for example `app password version 3 is not supported; upgrade
Materialize to use this password`, rather than the current single
`invalid app password format`. This is the message a customer sees when a token
minted by a newer console reaches an older `balancerd` or `mz` CLI, and "invalid
format" would send them to rotate a password that is fine.

`Display` round-trips the version. A `V0` password renders exactly as it does
today, so `mz`'s config file, which stores `app_password.to_string()`, does not
silently upgrade a legacy token into a v1 token with a fabricated tenant. Note
that `Display` remains lossy for legacy input in the way it already is: a
hex-with-separators token renders back as canonical `V0` base64.

The deterministic-rejection argument above is proved against the Rust parser
only. Two other parsers exist in the fleet, in cloud and in the Terraform
provider, and they do not necessarily branch on length the same way. Before v1
minting is enabled anywhere, each must be checked to confirm it rejects a v1
token rather than mis-decoding it. That check is a precondition on CLO-228 and
CLO-229, not an assumption of this design.

### Ergonomics

The token is `[A-Za-z0-9_-]` throughout, one word, no padding, no separators
beyond `_`. Nothing in it requires escaping in a libpq connection string, a URI
userinfo field, a shell word, or an unquoted YAML scalar. The leading character
is `m`, so it is never mistaken for a command-line flag.

On double-click selection, the honest position is that this is unchanged rather
than solved. `-` is in the URL-safe base64 alphabet and is not a word character
in browsers or in default terminal word-character sets, so a v1 token containing
a `-` selects only up to it. About 63% of v1 tokens contain at least one. That
is exactly the situation today's base64 tokens are in, so v1 is no worse, and
fixing it would mean leaving base64 for a longer encoding such as base32. Not
worth it for a property we have already shipped without.

### Security analysis

**The tenant ID becomes readable from the token.** Anyone holding a v1 password
can decode the tenant UUID with `base64 -d`.

This discloses nothing new to anyone who matters. A password holder already
learns their tenant ID by using the password: it is the `tenantId` claim of the
JWT Frontegg returns on exchange, and it is in the console URL. A third party
who has obtained the password has full database access, next to which a tenant
UUID is not the interesting part. There is no case where the tenant ID is secret
from someone who holds the password.

The remaining delta is a partial disclosure: a leak that exposes a token prefix
but not the whole token now reveals the tenant where it previously revealed the
client ID. Both are non-secret identifiers of the same account, so this is a
lateral move, not a downgrade. The field ordering above keeps the secret the
last thing such a leak reaches.

**The routing hint is client-controlled and `balancerd` acts on it before
authenticating.** A client can put any UUID in the tenant field and `balancerd`
will route to that tenant's `environmentd`. We accept this. Three reasons:

1. *It is not a new capability.* On the SNI path a client already picks the
   destination environment pre-authentication, and `balancerd` already routes on
   it with no authentication whatsoever. v1 brings the non-SNI path to the trust
   model the SNI path has always had, rather than introducing one.

2. *Mis-declaring the tenant mis-routes and cannot grant access.* `environmentd`
   is started with `--frontegg-tenant` and `validate_access_token` rejects any
   JWT whose `tenantId` claim differs, with `Error::UnauthorizedTenant`. So
   tenant A's credentials arriving at tenant B's `environmentd`, whether by a
   forged hint or a corrupted one, fail authentication at the destination. The
   client learns nothing beyond "authentication failed", which it could learn by
   guessing anyway. `balancerd` continues to pass the password through and
   `environmentd` continues to be the authority. Nothing about that changes.
   **This property is what makes the hint safe to trust for routing, and it must
   be asserted by a test, not assumed:** the mixed-fleet test suites should
   include a v1 password whose embedded tenant does not match its credentials
   and assert the connection is refused.

3. *The hint cannot be used for injection.* It is 16 raw bytes decoded into a
   `Uuid` and re-rendered canonically before substitution into `addr_template`.
   There is no path by which arbitrary characters reach the hostname, so it
   cannot be steered at a host Materialize does not operate. The address space
   the hint can select is exactly the set of tenant environments the template
   describes.

The genuinely new exposure is that unauthenticated input now reaches machinery
that previously ran only after a Frontegg exchange succeeded. Three concrete
consequences, all of which belong to CLO-227 and are listed here so they are
designed for rather than discovered:

- *Fan-out.* A well-formed string is enough to make `balancerd` resolve DNS and
  dial an arbitrary tenant's `environmentd`. This is a denial-of-service
  consideration, not an access-control one. It is bounded by `balancerd`'s
  existing per-source connection limits and by `environmentd`'s own limits and
  authentication, and it is the same exposure the SNI path already carries.
  Connection-limit tuning should be revisited as part of the change.
- *Metric label cardinality.* `balancerd` records
  `mz_balancer_tenant_pgwire_sni_count` with the tenant as a Prometheus label.
  Today the non-SNI path only ever supplies a Frontegg-validated UUID and the
  SNI path only a CNAME-derived one, both bounded. Populating that label from an
  unauthenticated hint lets a client mint unbounded label cardinality in a loop
  and blow up the metrics registry. The hint must not reach a metric label
  unvalidated. Bucketing unknown tenants into a single `unknown` label, as the
  code already does for the no-tenant case, is sufficient.
- *A dropped pre-authentication check.* `auth.authenticate(user, &password, ..)`
  today also validates that the pgwire user matches the identity in the JWT.
  Skipping the exchange skips that check, so `balancerd` forwards a mismatched
  user upstream. This is not an authentication bypass, since `environmentd`
  re-validates, but it does move a rejection from the edge to the backend and
  should be a deliberate choice rather than a side effect.

**Non-goals worth naming so they are not assumed.** The embedded tenant is not
evidence of anything. It must not be used for authorization, for audit records,
for metrics that are billed on, or for anything where a client asserting a false
value is a problem. Consumers that need a trustworthy tenant must keep reading
it from the validated JWT. The `AppPasswordVersion::V1` doc comment carries this
warning so it is visible at the point of use.

## Minimal Viable Prototype

The format is small enough that the prototype is the implementation, which is
CLO-226. `AppPassword` gains the version enum, `from_str` gains the length gate
and the versioned branch, and the existing round-trip unit test grows v1 cases
plus negative cases for unknown versions, for a v1 token fed to the legacy
heuristics, and for a legacy 43-character body that happens to begin `v1_`.

Two properties were checked before writing this document, and both hold:

- The previous parser rejects the 71-character sample token above. Verified by
  running that token through `AppPassword::from_str` as it exists on `main`.
- Legacy base64 bodies beginning with a version marker are real, not
  theoretical. Verified by sampling two million synthetic legacy tokens, which
  produced 80 collisions, one in 25,000. This is what forces the length check
  ahead of the marker check.

Both should land as permanent tests alongside CLO-226.

## Alternatives

**A new top-level prefix (`mzp1_`, `mzpv1_`).** Cleanly unambiguous and
self-evident to a human. Rejected because it breaks every credential redactor
that matches on `mzp_`, which is a security regression traded for cosmetics.

**Longer base64 with no marker, disambiguated by length alone.** Zero overhead
bytes. Rejected because it makes every future version a fresh compatibility
puzzle, and because the legacy hex branch accepts anything at least 64
characters long, so a length-only scheme relies on the payload happening not to
be 64 hex-looking characters. That is a probabilistic argument where a
deterministic one is available for three characters.

**A self-describing encoding (protobuf, CBOR, MessagePack) in the payload.**
Free extensibility. Rejected on size and on blast radius. The token grows, the
encoding is no longer fixed-width so length can no longer help disambiguate, and
a hostile-input parser runs at an unauthenticated edge in `balancerd`. A
fixed-offset layout is auditable in one table. New fields get a new version
number, which we need anyway.

**Signing or encrypting the payload.** Would make the hint trustworthy rather
than advisory. Rejected: it requires key distribution to every parser including
the console and the Terraform provider, it makes tokens key-rotation-sensitive
when they are supposed to be immutable forever, and it buys nothing, because
`environmentd`'s tenant check already ensures a false hint cannot do worse than
mis-route.

**Embedding the tenant in the client ID's unused bits.** No format change at
all. Rejected: the client ID is Frontegg's, we do not control its bit layout,
and there are no unused bits to take.

## Consumers that must adopt the format

**Parsers, in the materialize repo:**

- `src/frontegg-auth/src/app_password.rs`: the canonical parser and formatter.
  CLO-226.
- `src/balancerd/src/lib.rs`: `BalancerResolver::MultiTenant`, the reason for
  the change. Uses the hint when present, falls back to the Frontegg exchange
  for `V0`. CLO-227.
- `src/mz/src/context.rs`: parses the stored password.
- `src/mz/src/command/profile.rs`: writes `app_password.to_string()` back to the
  config file, so version must round-trip or a stored legacy password would be
  rewritten as something else.
- `src/frontegg-auth/src/auth.rs`: keys its session cache on the whole
  `AppPassword`. The same Frontegg credential presented once as `V0` and once as
  `V1` becomes two cache entries with two independent refresh loops, doubling
  Frontegg refresh traffic for that credential during any mixed period. Bounded
  and temporary, but worth knowing before it shows up in a graph.

**Parsers, outside this repo:**

- **cloud**: `infra/cli/parse_app_password.py` and the other parse sites.
  CLO-228.
- **terraform provider** (`terraform-provider-materialize`): parses app
  passwords and constructs them from the Frontegg API response. CLO-229.

**Minters.** Note that the two places `mz` obtains a password build it from
`clientId` and `secret` rather than from a token string, so they cannot produce
v1 without also being handed a tenant:

- **console**: `formatAppPassword()` in `console/src/queries/frontegg.ts` builds
  `mzp_${clientId}${secret}` in the browser and is where most customer passwords
  are born. It needs the tenant, which the console already has. This is the
  switch that starts the v1 fleet, so it lands last.
- `src/frontegg-client/src/client/app_password.rs`: builds an `AppPassword` from
  the Frontegg admin API's `{clientId, secret}` response, for `mz app-password
  create` and `mz app-password list`.
- `src/mz/src/server.rs`: builds one from the browser login callback's query
  params, for `mz profile init`.
- **cloud**: server-side minting. CLO-228.
- **terraform provider**: the `materialize_app_password` resource. CLO-229.
- `src/frontegg-mock/src/models/user.rs` and
  `test/balancerd/mzcompose.py`: test fixtures that hand-assemble
  `mzp_{client_id}{secret}`. These need v1 variants so mixed-fleet behaviour is
  actually exercised.

**A redactor that does not survive v1.** `test/limits/mzcompose.py` scrubs
tracebacks with `re.sub(r"mzp_[a-z0-9]*", ...)`. That character class covers the
lowercase-hex form the file mints today, but against `mzp_v1_...` it matches
only `mzp_v1` and stops at the `_`, leaving the entire payload in the log. It
must be widened to `[A-Za-z0-9_-]*` as part of this work. The other redactors
listed earlier all match greedily enough to be unaffected, which is what the
argument for keeping the `mzp_` prefix rests on.

**Docs and examples:** `doc/user/content/integrations/cli/reference/app-password.md`,
`.../cli/configuration.md`, `.../sql-clients.md`,
`doc/user/content/security/cloud/users-service-accounts/create-service-accounts.md`,
`src/mz/README.md`, and the `mzp_` sample strings in the integration guides.
Sample tokens should be updated to v1 shape so that copy-paste of a doc example
exercises the new path.

**Rollout order.** Parsers everywhere first, then minters. The constraint that
makes this more than a checklist is the `mz` CLI: customers run versions we
cannot upgrade, and every one of those predates this design, so a v1 token
handed to an old `mz` fails with `invalid app password format` and no hint about
why. The versioned error message helps the *next* rollout, not this one. So
either the console keeps minting `V0` for the `mz`-facing flows until old CLI
versions have aged out, or v1 minting is opt-in per tenant until that is true.
Deciding which is CLO-228's call, informed by CLI version telemetry, but it
cannot be skipped.

## Open questions

None blocking. Three decisions are recorded above rather than left open, and are
called out here because they are the ones most likely to be re-litigated in
review: no region or environment hint in v1, no checksum, and legacy tokens win
the 43/44 length tie. The first two are additive in a future version if a
concrete need appears, and neither can be removed once shipped, which is why v1
omits them. The third is not a preference but a consequence of legacy tokens
being unrewritable.
