# Contributing to the Materialize Operator Helm Chart

## Versioning

For every change, increment the `version` contained in [Chart.yaml](https://github.com/cockroachdb/helm-charts/blob/master/cockroachdb/Chart.yaml). The `version` should follow the [SEMVER](https://semver.org/) versioning pattern.

- For changes that do not affect backward compatibility, increment the PATCH or MINOR version (e.g., `1.1.3` -> `1.1.4`).
- For changes that affect backward compatibility, increment the MAJOR version (e.g., `1.1.3` -> `2.0.0`).

Examples of changes that affect backward compatibility include:
- Major version releases of Materialize.
- Breaking changes to the Materialize chart templates.

## Chart Documentation

We use `helm-docs` to automatically generate and update the README for our Helm chart. After making any changes to the chart or its values, please run:

```bash
helm-docs
```

This command updates the `README.md` file based on the chart's `values.yaml` file and the `README.md.gotmpl` template.

Make sure you have `helm-docs` installed. You can install it by following the instructions [here](https://github.com/norwoodj/helm-docs).

## Linting

Before submitting a pull request, lint your changes using the `helm lint` command:

```bash
helm lint .
```

This will check your chart for potential issues or errors.
