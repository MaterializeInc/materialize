# CI Configuration Examples

This directory contains example CI/CD configurations for running Console tests against multiple Materialize versions.

## Files

- **`github-actions-example.yml`** - Example GitHub Actions workflow using matrix strategy
- **`buildkite-example.yml`** - Example Buildkite pipeline steps

## Choosing a CI System

### GitHub Actions (Recommended for Console Repo)

**Pros:**
- Native to GitHub
- Easy matrix builds
- Good caching support
- No additional infrastructure

**Use this if:**
- Console has its own CI pipeline
- You want independent test runs per PR
- Simple setup is preferred

### Buildkite (Recommended for Materialize Repo Integration)

**Pros:**
- Integrates with existing Materialize CI
- Can share build artifacts
- Flexible agent pools
- Better for monorepo-style testing

**Use this if:**
- Want to integrate with Materialize's existing pipeline
- Need access to pre-built Materialize binaries
- Running in Materialize's infrastructure

## Setup Instructions

### For GitHub Actions

1. Copy `github-actions-example.yml` to your console repo:
   ```bash
   cp ci-examples/github-actions-example.yml \
      ../../../console/.github/workflows/console-multi-version-tests.yml
   ```

2. Adjust the workflow:
   - Update repository paths if needed
   - Configure any required secrets
   - Adjust version matrix as needed

3. Commit and push:
   ```bash
   cd ../../../console
   git add .github/workflows/console-multi-version-tests.yml
   git commit -m "Add multi-version console SQL tests"
   git push
   ```

### For Buildkite

1. Add the steps from `buildkite-example.yml` to Materialize's pipeline:
   ```bash
   # Edit ci/test/pipeline.template.yml
   # Add the console multi-version steps from buildkite-example.yml
   ```

2. Ensure console repo is available:
   - Either checkout as sibling directory
   - Or add checkout step in pipeline

3. Test the pipeline:
   ```bash
   cd /Users/qindeel/dev/materialize
   ci/mkpipeline.sh > /tmp/pipeline.yml
   # Review /tmp/pipeline.yml
   ```

## Version Matrix Configuration

The default matrix tests:

| Alias | Description | Docker Image | Purpose |
|-------|-------------|--------------|---------|
| `cloud-backward` | Last released version | Published image | Test backwards compatibility |
| `cloud-current` | Current development | Source build | Test current development |
| `cloud-forward` | Forward compatibility | Source build | Test forward compatibility |
| `sm-lts` | Self-Managed LTS | Published image | Test LTS support |

### Customizing the Matrix

To add or remove versions, edit the matrix in your CI config:

**GitHub Actions:**
```yaml
strategy:
  matrix:
    include:
      - version_name: "Custom Version"
        version_alias: "v0.100.0"
```

**Buildkite:**
```yaml
- id: console-custom-version
  label: "Console Tests - Custom"
  env:
    MZ_VERSION: "v0.100.0"
```

## Troubleshooting

### Tests fail to start

**Problem:** `docker: Error response from daemon: pull access denied`

**Solution:** Ensure the specified version exists:
```bash
cd /Users/qindeel/dev/materialize/test/console
./resolve_version.py cloud-backward
# Check if this version exists on Docker Hub
```

### Console can't connect to Materialize

**Problem:** Connection refused or timeout

**Solution:** Ensure services are fully started:
```yaml
- name: Wait for Materialize
  run: sleep 10  # Add appropriate wait time
```

Or use health checks:
```bash
until docker exec console-materialized-1 pg_isready; do
  echo "Waiting for Materialize..."
  sleep 2
done
```

### Version resolution fails

**Problem:** `Unknown version alias: xyz`

**Solution:** Check available aliases:
```bash
./resolve_version.py --help
```

### Tests pass locally but fail in CI

**Common causes:**
- Different Docker images (version mismatch)
- Missing environment variables
- Timing issues (services not fully started)
- Resource constraints (memory/CPU)

**Debug:**
```yaml
- name: Debug Environment
  run: |
    docker ps
    docker logs console-materialized-1
    env | grep MZ_
```

## Performance Optimization

### Parallel Execution

Both CI systems support parallel execution:

**GitHub Actions:** Matrix jobs run in parallel automatically

**Buildkite:** Add `parallelism` key:
```yaml
- id: console-tests
  parallelism: 4
```

### Caching

**Cache Docker images:**
```yaml
# GitHub Actions
- name: Cache Docker images
  uses: actions/cache@v4
  with:
    path: /tmp/docker-images
    key: docker-${{ matrix.version_alias }}
```

**Reuse Materialize builds:**
- For `cloud-current` and `cloud-forward`, depend on build job
- For published versions, images are cached by Docker

### Resource Allocation

Recommended agent specs:
- **Minimum:** 4 CPU, 8GB RAM
- **Recommended:** 8 CPU, 16GB RAM
- **Storage:** 50GB for Docker images

## Monitoring and Alerts

### Success Metrics

Track these metrics:
- Pass rate per version
- Test duration
- Failure patterns (which tests fail on which versions)

### Alerting

Set up notifications for:
- Any test failure on `cloud-current` (blocks PR)
- Multiple failures on `cloud-backward` (compatibility issue)
- Failures on `sm-lts` (support issue)

### Reporting

Consider adding:
- Test result badges to README
- Slack/email notifications on failure
- Trend reports (pass rate over time)

## Next Steps

1. Choose your CI system (GitHub Actions or Buildkite)
2. Copy and customize the example configuration
3. Test locally first: `./test-with-version.sh cloud-backward`
4. Deploy to CI and monitor first run
5. Iterate on timing, resources, and version matrix as needed

