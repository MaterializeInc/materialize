# Usage and billing impersonation

- Associated: [MaterializeInc/cloud#8562](https://github.com/MaterializeInc/cloud/pull/8562/files)

## The Problem

The console is not able to impersonate an organization to retrieve usage and billing data. This ability is needed to develop cost reporting features for customers.

## Success Criteria

Internal users are able to use impersonation to view a customer's usage and billing data in the console without any action by the customer. Console developers are able to use impersonation to further develop cost reporting features using usage and billing data.

## Out of Scope

Including the FrontEgg JWT is not required for impersonation. The global-api will handle making API calls on behalf of console.

## Solution Proposal

Cloud will create a new teleport app for the global-api. This is necessary because the environmentd teleport app exists in a different kubernetes cluster.
A new internal endpoint `/redirect` will be added to the global-api. Console will pass in the existing impersonation environmentd teleport app url ending in `internal-console`. This will authenticate to both teleport apps and allow console to call the global-api endpoints as well as environmentd.

Since the global-api is a different origin, console `GET` requests will need to ensure a simple request is made that does not trigger pre-flight checks.
[MDN CORS Simple Requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS#simple_requests) The organization_id will need to be passed in the `Accept` header.

Example:
- console loads in impersonation mode for the environmentd Internal HTTP API teleport app
- console changes the window location and initiates a login to the global-api teleport app url with a redirect back to impersonated console
- console can now make GET requests to the global-api and session information will be attached as a simple request

Alternatively, the above could be simplified by adjusting the Grafana Org Look Up dashboard Console Impersonation link. The link would instead point to the global-api with a redirect such as this example from my personal stack: `https://global-api-aws-us-east-1-eph-dev.materialize.teleport.sh/redirect?to=https%3A%2F%2Faws-us-east-1-txzur43pwfcklgvqfix6cr5gqm-0.materialize.teleport.sh%2Finternal-console`

## Dependencies

### Adapter

No work will be required by the adapter team. A workaround has been found to allow console to directly send `GET` requests to the global-api while teleport works on implementing support for pre-flight checks.

### Cloud

No changes are anticipated for the existing "Environmentd Internal HTTP API" teleport app. A new Teleport app for the global-api will be created. Existing Teleport internal users who have access to console impersonation will be given permissions to this app as well.

The cloud team will add an additional internal port for the global-api serving all read-only endpoints. The internal port will use separate auth middleware and check the `Accept` header. This organization id will be used for retrieving data only for the specific customer. Once Teleport implements correct handling for pre-flight checks, the `X-Materialize-Organization-Id` header will be used instead.

This follow up work is being tracked at [use custom header on internal global-api routes middleware #8689](https://github.com/MaterializeInc/cloud/issues/8689).

Teleport audit logs will be available since console will directly auth and make requests to the global-api. The teleport app session log will show the internal user and the session_chunk_id. The session_chunk_id can be used to view the api calls made to the global-api.

## Minimal Viable Prototype

Not applicable. The required upstream work will need to be merged before console development.

## Alternatives

Initially this design doc outlined an approach of proxy'ing global-api calls through environmentd. Teleport has found a path forward to support pre-flight checks so the simple request work around was chosen as the path forward.

## Open questions

None at this time.
