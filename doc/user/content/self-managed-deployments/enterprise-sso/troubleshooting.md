---
title: "Troubleshooting"
description: "Diagnose and fix common issues with the Ory-based enterprise SSO stack."
menu:
  main:
    parent: "enterprise-sso"
    identifier: "enterprise-sso-troubleshooting"
    weight: 70
---

Common errors and fixes when deploying or operating the Ory stack.

## Where to look first

For most failures, the right place to start is the Ory pod logs:

```bash
kubectl logs -n ory deploy/kratos -f
kubectl logs -n ory deploy/hydra -f
kubectl logs -n ory deploy/ory-selfservice-ui -f
kubectl logs -n ory deploy/polis -f          # when enable_polis = true
```

Hydra Maester (responsible for managing OAuth2Client CRDs):

```bash
kubectl logs -n ory deploy/hydra-hydra-maester -f
```

For login-time issues, tail Kratos, Hydra, and Polis simultaneously so
you can see which component rejected the flow.

## Symptom table

| Symptom | Likely cause | Fix |
|---|---|---|
| Pods stuck in `ImagePullBackOff` | License key JWT missing the `ory` entitlement, or expired | Update `license_key` in tfvars, re-apply, restart the Ory pods |
| `curl https://polis.example.com` times out, LB has zero endpoints | Service selector doesn't match polis pod labels | Confirm the LoadBalancer Service selector targets `app.kubernetes.io/name=polis, instance=polis` |
| TLS handshake fails on polis hostname | Polis doesn't terminate TLS, the LB target is plain HTTP | The example deploys a pingap TLS proxy in front of Polis automatically; check that the `polis-tls-proxy` Deployment is running |
| Polis SCIM endpoint URLs return `http://localhost:5225/...` | `EXTERNAL_URL` env var not set on Polis | Module sets it automatically from `external_url`; re-run `terraform apply` |
| Polis logs `OAuth server not configured correctly for openid flow, check if JWT signing keys are loaded` | `OPENID_RSA_PRIVATE_KEY` and `OPENID_RSA_PUBLIC_KEY` missing | Module auto-generates and injects them; re-run `terraform apply` |
| Polis logs `"pkcs8" must be PKCS#8 formatted string` | RSA private key was PKCS#1 | Module uses the PKCS#8 form; re-run `terraform apply` |
| First login fails with `no matching authentication claim found in the JWT` | User clicked Allow on the consent screen without ticking the `email` scope | Module sets `skipConsent: true` on the OAuth2Client which bypasses the consent screen for the Materialize client; re-run `terraform apply` |
| `"Couldn't fetch XML data"` when registering a Polis SAML connection | The IdP's metadata URL is gated by API auth | Post `rawMetadata=<XML>` to Polis instead of `metadataUrl=...` |
| "Sign in via SAML" button missing on Kratos login | Cached login flow from before the polis provider was added | Hard refresh or open a new incognito session |
| Materialize console reaches the login screen but balancerd times out | DNS or cert SAN mismatch | Confirm the balancerd hostname A record resolves and is in the cert SAN list (`balancerd_extra_dns_names`) |
| User logs in but can't run any SQL | JIT role created with no privileges | Run `GRANT <role> TO "user@email"` as `mz_system` |
| SCIM "Test Connector Configuration" passes but no users push | Existing assignments don't backfill when SCIM is enabled after-the-fact | In Okta, unassign + reassign the user, or push profile updates from the people side |
| `terraform destroy` hangs on `kubernetes_namespace.ory` | OAuth2Client finalizer not cleared because Hydra Maester is torn down before processing it | `kubectl patch oauth2client materialize-oauth2-client -n ory --type=json -p='[{"op":"remove","path":"/metadata/finalizers"}]'` then re-run destroy |

## Detailed walkthroughs

### Hydra returns `invalid_client` on the token endpoint

Usually means the OAuth2Client CRD didn't reconcile against Hydra (so
the `client_id` Materialize is using doesn't exist in Hydra's
database).

Check the CRD:

```bash
kubectl get oauth2client -n ory materialize-oauth2-client -o yaml
```

Look for the `status.reconciliationError` field. Common errors:

- "Hydra admin unreachable" -- Hydra Maester can't connect to Hydra's
  admin port. Confirm `hydra-admin.ory.svc.cluster.local:4445` resolves
  from inside the cluster.
- "duplicate client name" -- a stale OAuth2Client from a previous apply
  exists in Hydra's DB. Delete the CRD, wait for Hydra Maester to drop
  the Hydra-side record, then re-apply.

Check Hydra Maester logs:

```bash
kubectl logs -n ory deploy/hydra-hydra-maester --tail=100
```

### Kratos selfservice UI shows a blank login page

Almost always a TLS or DNS issue between the browser and Kratos. Open
your browser's network tab and inspect the requests:

- 502/504 on `/self-service/login/browser` → Kratos isn't reachable
  from the UI pod. Check Kratos pod status.
- TLS error on the redirect target → cert not provisioned yet, or the
  hostname DNS record isn't propagated. Run `kubectl get certificate -A`
  and confirm `kratos-tls` is `READY=True`.
- `CORS` error → the Hydra `cors_allowed_origins` doesn't include the
  console hostname. Check the `hydra` Helm release values.

### Polis SAML flow returns `server_error: <random-name>`

Polis encodes errors with a random nickname for the log entry (e.g.
`curve_tourist_bean`). The real error is in the Polis pod logs:

```bash
kubectl logs -n ory deploy/polis --tail=200 | grep -B2 -A5 "error\|Error"
```

Common causes:

- `"pkcs8" must be PKCS#8 formatted string` → see symptom table
- IdP metadata doesn't include the SAML signing cert → re-export the
  metadata XML from the IdP and update the Polis connection with
  `rawMetadata`
- Audience mismatch → confirm the IdP's SAML app audience is set to
  `https://saml.boxyhq.com`

### Console login loops back to the IdP indefinitely

Usually a redirect URI mismatch. The redirect URI registered with your
IdP must exactly match what Kratos sends:

```
https://<your-kratos-hostname>/self-service/methods/oidc/callback/<id>
```

where `<id>` is the entry in `upstream_oidc_providers`. Trailing slashes
matter. Update the redirect URI in the IdP to match.

If the redirect URI is correct, check the `oidc_audience` system
parameter on the Materialize side. It must include the OAuth2 client_id
Hydra Maester generated:

```bash
kubectl get secret -n ory materialize-oauth2-client \
  -o jsonpath='{.data.CLIENT_ID}' | base64 -d
```

Compare with the Materialize CR's `system_parameters.oidc_audience`.

### A specific cloud is hanging during destroy

See the cloud-specific notes:

- **Azure**: usually the OAuth2Client finalizer (see the symptom table
  above)
- **AWS**: the AWS Load Balancer Controller can race with namespace
  deletion. See [AWS-specific notes](/self-managed-deployments/enterprise-sso/install-on-aws/#cleanup)
- **GCP**: the GKE master IP allocator can be slow; usually patience is
  the fix

## When to escalate

If the failure doesn't match any of the above and the Ory pod logs
don't surface a clear cause, file an issue with:

- The pod logs (`kubectl logs -n ory deploy/<component>`) from the
  affected component plus the two it talks to
- The Hydra OAuth2Client CRD YAML (`kubectl get oauth2client -n ory
  materialize-oauth2-client -o yaml`)
- The output of `kubectl get all,certificate,oauth2client -n ory`
- The cloud (Azure / GCP / AWS), Materialize version, and Ory chart
  version pinned in your tfvars

## See also

- [Ory Kratos troubleshooting](https://www.ory.sh/docs/kratos/troubleshooting)
- [Ory Hydra debugging](https://www.ory.sh/docs/hydra/debug)
- [Ory Polis docs](https://www.ory.sh/docs/polis)
