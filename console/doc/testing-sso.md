# OIDC Debugging

The OpenID Connect (OIDC) authentication protocol allows users to log into the Console via SSO. They can initiate the process by following the login flow with the email of an OIDC account. We are not able to use OIDC on our development server since it relies on a staging and production domain. We can get around this by deploying to a deploy branch.

## How to debug in development

1. Make sure you have access to an OIDC account. We have a shared OIDC account in 1Password with the email infra+oidc-test@mtrlz.com
2. Point your changes to the branch `deploy/oidc-test` and force push there. Vercel auto deploys this branch to **[oidc-test.console.materialize.com](http://oidc-test.console.materialize.com/)** which allows OIDC sign in. We push to this branch since each test account's organization redirect URLs are configured to redirect to **[oidc-test.console.materialize.com](http://oidc-test.console.materialize.com/)**
3. Keep pushing your changes to this branch to debug and make sure noone else is pushing to it too

# SAML Debugging

Similar to OIDC, but you'll have to login via the shared SAML account with email infra-saml-test@mtrlz.io. You'll still need to push to the `deploy/oidc-test` branch and test via **[oidc-test.console.materialize.com](http://oidc-test.console.materialize.com/)**.

# Google Signups

To test Google signups, we're only allowed to signup when we have an `@materialize.com` domain. What we can do is, if you have permissions, remove your `@materialize.com` email in all accounts in the Frontegg portal and signing up via Google will trigger a signup and create another account. To test these changes, you can push to the `deploy/oidc-test` branch.
