{{< note >}}
`terraform destroy` can hang on the `ory` namespace because the `OAuth2Client` CRD has a Hydra Maester finalizer that is not always cleared before Maester itself is torn down. If the destroy stalls on the namespace, patch the finalizer off:

```bash
kubectl patch oauth2client materialize-oauth2-client -n ory \
  --type=json -p='[{"op":"remove","path":"/metadata/finalizers"}]'
```

Then re-run `terraform destroy`. A cleaner fix is tracked upstream.
{{</ note >}}
