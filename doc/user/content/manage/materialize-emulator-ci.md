---
title: "Using Materialize Emulator in CI"
description: "Learn how to integrate Materialize Emulator into your continuous integration (CI) workflows for automated testing of applications that use Materialize."
menu:
    main:
        parent: "manage"
        weight: 11
        name: "Using Materialize Emulator in CI"
---

The Materialize Emulator is well-suited for continuous integration (CI) environments, allowing you to test applications that depend on Materialize without requiring a full deployment. This guide explains how to set up and use the Materialize Emulator in GitHub Actions and other CI systems.

{{< note >}}
The Materialize Emulator is designed for testing and evaluation purposes. It is not representative of Materialize's full performance and feature set, and is not suitable for production workloads.
{{</ note >}}

## Using Materialize Emulator in GitHub Actions

GitHub Actions provides a convenient way to automate your testing workflows. Here's how to integrate the Materialize Emulator into your GitHub Actions workflow:

### Setting up Docker Compose

First, create a `docker-compose.yml` file in your repository to define the Materialize Emulator service:

```yaml
version: "3"
services:
    materialize:
        image: materialize/materialized:latest
        ports:
            - "6875:6875" # SQL interface
        healthcheck:
            test: ["CMD", "curl", "-f", "http://localhost:6875/api/readyz"]
            interval: 5s
            timeout: 5s
            start_period: 35s
```

This configuration:

-   Uses the latest Materialize Emulator image
-   Exposes the necessary ports for SQL connections and health checks
-   Defines a health check to ensure Materialize is ready before running tests

### Creating a GitHub Actions Workflow

Next, create a GitHub Actions workflow file (e.g., `.github/workflows/ci.yml`) that uses the Docker Compose configuration:

```yaml
name: CI Tests with Materialize

on:
    push:
        branches:
            - main
    pull_request:
        branches:
            - main
jobs:
    test:
        runs-on: ubuntu-latest
        steps:
            - name: Checkout Repository
              uses: actions/checkout@v4

            - name: Start Materialize with Docker Compose
              run: docker-compose up -d

            - name: Wait for Materialize to be Ready
              run: |
                  echo "Waiting for Materialize to be ready..."
                  timeout 60s bash -c 'until curl -fsS localhost:6875/api/readyz; do sleep 1; done' || exit 1
                  echo "Materialize is ready!"

            - name: Run Tests
              run: |
                  # Replace this with your actual test command
                  ./run-tests.sh
            - name: Tear Down Materialize
              run: docker-compose down
```

This workflow:

1. Checks out your repository
2. Starts Materialize using Docker Compose
3. Waits for Materialize to be ready by polling the health check endpoint
4. Runs your tests
5. Tears down the Materialize container

## Using Materialize Emulator in Other CI Systems

The approach for other CI systems is similar to GitHub Actions:

## Connecting to Materialize in Tests

In your test code, you can connect to the Materialize instance using the following connection details:

```
Host: localhost
Port: 6875
User: materialize
Database: materialize
```

## Troubleshooting

### Common Issues

-   **Error: Failed to initialize container**: Increase the timeout duration in the health check.

## Next Steps

-   Learn more about [Materialize SQL](/sql/) to write effective tests.
-   Explore [Materialize sources](/sql/create-source/) for ingesting test data.
-   Check out the [Quickstart](/get-started/quickstart) for more examples.
