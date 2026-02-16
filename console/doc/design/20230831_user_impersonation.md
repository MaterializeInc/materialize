# Organization impersonation

- Associated: MaterializeInc/console#370

## The Problem

The console has many features that are useful for diagnosing and debugging
issues in customer environments, however currently you have to be invited to a
user's Frontegg organization in order to view their environment in the console.

## Success Criteria

Internal users are able to view any customer's environment in the console
without any action by the customer.

## Out of Scope

Access to a customer's Frontegg data and features is not in scope for this
design. These features will be disabled when impersonating to avoid confusion.

Likewise, features that rely on cloud region and global APIs, such as enabling
regions, account blocking, viewing invoices will be disabled during
impersonation.

## Solution Proposal

From the [Organization retool app][1], users will select the customer
environment they want to connect to, which will redirect them to a new
environmentd endpiont on the new teleport environmentd app:
`$CLOUD_PROVIDER-$CLOUD_REGION-$ORG_ID-$ENV_ORDINAL-environmentd.materialize.teleport.sh/internal-console`.
For consistency, the dynamic portion of the subdomain follows the
[format materialize uses][4] to identify environments.

This will authorize the user's browser to access the customer environmentd by
setting a teleport auth cookie, and the `/internal-console` path will redirect
to `$ORG_ID.$REGION_SLUG.internal.console.materialize.com`.

When console initializes, we will look at the url, and if it matches the
pattern above we will set a new config variable, `impersonatedOrganization`,
which will have the ID and region from the url.

The `executeSql` function will check `impersonatedOrganization`, and use
the value to build the teleport environmentd url.

Other than environmentd, console uses the cloud region API to enable regions
and get region status. While impersonating, these features will be disabled.
The region will be set based on the url, and we will assume the region is
always enabled.

Teleport gives us audit logging of ever user session, however environmentd will
only be aware of someone connected as mz_support, not the actual user that
logged in. To figure that out, we would have to correlate timestamps between
teleport and environmentd.

## Dependencies

Before we can do the console work, we need the some upstream changes.

### Adapter

A new environmentd http route `/internal_console` which should ideally should
only be accessible from on the internal http interface. All it needs to do is
redirect to
`$CLOUD_PROVIDER-$CLOUD_REGION-$ORG_ID-$ENV_ORDINAL-environmentd.materialize.teleport.sh/internal-console`.
This url will be constructed by the environment controller and passed a CLI
parameter to environmentd.

The existing internal http interface will need to be modified to allow
connecting as the `mz_support` user, rather than `mz_system`. To support this,
environmentd should accept a custom header `X-Materialize-User`, that can be
either `mz_support` or `mz_system`. Teleport will be configured to always
inject this header, set to `mz_support` so users can't escalate their
privileges. If the header is not specified, we should fall back to the existing
behavior of authenticating as `mz_system`.

### Cloud

We need to create a new set of teleport applications, one per enabled customer
region:
`$CLOUD_PROVIDER-$CLOUD_REGION-$ORG_ID-$ENV_ORDINAL-environmentd.materialize.teleport.sh`.
This application should point to the internal http port on the environmentd
instance for that customer region.

This logic will be very similar to how we create our existing
[teleport resources dynamically](2), but instead would create an application
resource that points to the environmentd internal HTTP port using [Teleport
dynamic app registration][3].

Also, we need to add the CORS allowed origin
`*.internal.console.materialize.com` to environmentd pods.

Finally, we will need to add a button in the retool organizations app that
links to the `/internal-console` environmentd endpoint.

## Minimal Viable Prototype

I can't think of a good way to prototype this without the ustream work,
unfortunately.

## Alternatives

The simplest way to build this is for users to run a local tsh proxy:

```
tsh proxy app $ORG_ID.$REGION_SLUG.environmentd
```

Then click a button (maybe re-using the existing stack switcher?) to point to
the local proxy.

This still requires all the changes in cloud and adapter outlined above, except
adding a button to the retool organization app. It does save some work in
console, but not that much.

## Open questions

Is the teleport agent running in the system cluster able to access the internal
envd port? If not, we will have to install the teleport agent into each env
cluster.

[1]: https://retool.dev.materialize.com/apps/Organizations
[2]: https://github.com/MaterializeInc/cloud/blob/d93604e4be9f09eed3d41c0bb76f7299107fa76e/src/environment-controller/src/controller/teleport.rs#L114C33-L114C33
[3]: goteleport.com/docs/application-access/guides/dynamic-registration
[4]: https://github.com/MaterializeInc/materialize/blob/37b708fb46642aaa00695b557ef509435c96c191/src/sql/src/catalog.rs#L881-L910
