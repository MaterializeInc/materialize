# Contributing to the Materialize Operator Helm Chart

## Versioning

For every change, increment the `version` contained in [Chart.yaml](https://github.com/materializeinc/materialize/blob/main/misc/helm-charts/operator/Chart.yaml). The `version` should follow the [SEMVER](https://semver.org/) versioning pattern.

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

If you want to ignore a specific section in the `values.yaml` file, you can add the `# @ignored` tag to the section you want to ignore.

## Linting

Before submitting a pull request, lint your changes using the `helm lint` command:

```bash
helm lint .
```

This will check your chart for potential issues or errors.

## Unit Testing

We use the `helm-unittest` plugin for unit testing our Helm chart. This helps ensure that our chart templates are rendering correctly and that changes don't introduce unexpected issues.

### Setting Up helm-unittest

If you haven't already installed the `helm-unittest` plugin, you can do so with the following command:

```bash
helm plugin install https://github.com/quintush/helm-unittest
```

### Running Unit Tests

To run the unit tests, use the following command from the root of the chart directory:

```bash
helm unittest .
```

This will run all test files located in the `tests/` directory.

### Writing Unit Tests

Unit tests are written in YAML and should be placed in the `tests/` directory. Each test file should focus on a specific template or set of related templates.

Here's a basic structure for a test file:

```yaml
suite: test <template-name>
templates:
  - <template-name>.yaml
tests:
  - it: should <do something>
    set:
      <value-to-set>: <value>
    asserts:
      - <assertion>
```

For more detailed information on writing tests, refer to the [helm-unittest documentation](https://github.com/quintush/helm-unittest).

### Continuous Integration

TODO: Implement CI pipeline for running unit tests and linting.
