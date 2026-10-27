# Replace Frontegg React Components

- Associated: https://github.com/MaterializeInc/console/issues/1398

## The Problem

The `@frontegg/react` package is very opinionated and poorly optimized. Frontegg
initialization often takes more than five seconds even on a gigabit connection, which is
slow even by modern SPA standards. Using their components for our authentication flows
also severely limits our ability to customize the interface.

## Success Criteria

1. Replacement of the Frontegg react components for all interactions other than the admin
   panel. Since we can initialize the admin panel asynchronously in the background, it's
   much less of a concern.

2. The Frontegg provider component should not block rendering. This component has to make
   more than a dozen API calls before most of our components even mount. Instead, we
   should be able to make a single refresh token api call before we can start calling
   Materialize APIs.

3. Initial page load should time should be reduced by at least 1 second. There is more
   optimization work that we can do, but just getting the Frontegg provider component out
   of the critical path should give us this.

## Out of Scope

Replacing the admin panel. It's too much surface area to bite off, plus it's not in the
critical path.

## Solution Proposal

We've already laid some of the groundwork for this proposal. We will use react query to
manage all the Frontegg state. This gives us consistency with our other API calls, as
well as automatic retries, and fine grained caching with invalidation. Nearly all the
usages of the Frontegg react hooks have already been replaced with such calls already.

The Frontegg access token is already stored globally in module state, which allows us to
automatically refresh the token on 401 responses and then retry the original request.
Currently we have logic to sync the Frontegg provider managed token to this module state,
and I think we will need to continue doing this as long as we use the Frontegg admin
panel.

### Routes

The rest of the work is mostly handling each of the routes for various authentication
flows.

#### Login: `/account/login`

The login page. The prototype has a basic version, but we will have to style it, as well
as supporting SSO, social login, and linking to sign up. It calls the login API and
redirects to the console root or the redirectUrl if one is set.

`POST /identity/resources/auth/v1/login`

If MFA is required, we post the code to the MFA verify endpoint. Note that we are only
implementing authenticator app support, if we ever enable other MFA types, it will
require additional work.

`POST /identity/resources/auth/v1/user/mfa/verify`

#### Logout: `/account/logout`

This route calls the Frontegg logout API and then redirects to sign in.

`POST /identity/resources/auth/v1/logout`

#### Activate account: `/account/activate`

This is a form for newly invited users to set their password. On submit, call the
activate user API with user ID and token from the query string, plus the provided
password from the form.

`POST /identity/resources/users/v1/activate`

#### Accept invitation: `/account/invitation/accept`

A route which handles existing users being invited to a new tenant (organization). The
user will get an email with the accept link, which will include the user ID and token as
query string parameters, which will be sent to the accept invitation API. On success
redirect to console root.

`POST /identity/resources/users/v1/invitation/accept`

#### Forgot password: `/account/forget-password`

A forgot password form that accepts the users email and calls the reset password API,
which will send a reset password email to the user.

`POST /identity/resources/users/v1/passwords/reset`

#### Reset password: `/account/reset-password`

When following the reset password link from the email above, the user lands here, where
they enter their new password. On submit, call the reset password API, show a success
message, then redirect them to the sign in.

`POST /identity/resources/users/v1/passwords/reset/verify`

#### Social login: `/account/social/success`

Part of an oauth flow, this is the route a user lands on after signing in through Github
or Google. The url will include query params for oauth success. On load call the SSO
postlogin API with `code`, `redirectUri` and `state` as query params. The response is
similar to the refresh token api, specifically the `accessToken` will be stored in global
state for future API requests. If we get a 302 response from these endpoints, we get a URL as the body
instead of the Location header and navigate via JavaScript. This is how Frontegg does it.

`POST /identity/resources/auth/v1/user/sso/google/postlogin`

#### OIDC login: `/account/oidc/callback`

This the callback from an identity provider on successful OIDC login. Similar to the
social login endpoint, the url will include `code` and `state`. Console should forward
those params on as `Code` and `RelayState`, and Frontegg will respond with a refresh
token cookie.

`POST /identity/resources/auth/v2/user/oidc/postlogin`

#### Sign up: `/account/sign-up`

A signup form, which calls the signup API.

`POST /identity/resources/users/v1/signUp`

### Other Frontegg APIs

#### Prelogin: `/frontegg/identity/resources/auth/v2/user/sso/prelogin`

This request checks if a given email is configured for SSO, and returns the identity
provider address and type.

### Testing

All of the routes described above will have RTL tests that will verify the forms work as
expected and the correct API calls are made. The simple email / password flow is already
tested by the e2e tests.

We will also need to manually test each user flow. All the basic flows can be tested in
staging, but OIDC and SAML are only configured in production. SAML is enabled for
employees through Okta, so that's easy enough, and we have a test OIDC account in
1Password.

`POST /identity/resources/users/v1/signUp`

### Rollout plan

Since we this change affects the sign up and sign in flows, we can't just use
LaunchDarkly to control the rollout of this feature. Instead, we will sync the flag state
to local storage. When Console loads, it will check the local storage value and cache the
result in module state. This module value will be used to determine if we should load
Frontegg components or our new custom components. Caching the local storage value for the
duration of a session is important because we don't want to unmount the Frontegg provider
in the middle of a session, as it would be very disruptive.

With this design, we will be free to use LaunchDarkly to roll the feature out
incrementally, similar to any other feature. We will start with staging, then enable it
for internal organizations. Once we are confident in it, we can start enabling it for our
risk tolerant organizations, and finally our risk averse organizations.

## Minimal Viable Prototype

This [PR](https://github.com/MaterializeInc/console/pull/3009) does the minimal work to
have a login page and removes the Frontegg provider completely when the feature flag is
enabled.

## Alternatives

This design mostly fell out of existing design choices, in particular using react query
for server state management. We've been really happy with this choice for calling both
cloud APIs and the environmentd sql API, so it's any easy call.

## Open questions

I'm not particularly familiar with the SAML and OIDC flows, so it's possible I've missed
something there. The Frontegg client code defines callbacks routes for both, but SAML
appears to be handled server side with our configuration.
