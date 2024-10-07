# Contributing to the Materialize Operator Helm Chart

## Chart Documentation

We use `helm-docs` to automatically generate and update the README for our Helm chart. After making any changes to the chart or its values, please run:

```bash
helm-docs
```

This command will update the README.md file based on the chart's `values.yaml` file and the `README.md.gotmpl` template.

Make sure you have helm-docs installed. You can install it by following the instructions [here](https://github.com/norwoodj/helm-docs).

## Linting

Before submitting a pull request, please lint your changes using the `helm lint` command:

```bash
helm lint .
```

This will check your chart for possible issues or errors.
