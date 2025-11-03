# Console Test Fixture

This directory contains mzcompose workflows for testing the Materialize console.

## Basic Usage

Start Materialize and dependencies for console testing:

```bash
./mzcompose down -v
./mzcompose run default
```

Then in the console repository, run:
```bash
cd ../../../console  # or wherever your console repo is
yarn test:sql
```

When done:
```bash
./mzcompose down -v
```

## Available Workflows

### `default` - Start Services (Current Source)

Starts Materialize (current source build) and supporting services.

```bash
./mzcompose run default
```

### `start-version` - Start Services with Specific Version

Starts services with a specific Materialize version for console testing.

```bash
./mzcompose run start-version cloud-backward
./mzcompose run start-version sm-lts
./mzcompose run start-version v0.147.18  # Or direct version
```

**Arguments:**
- `cloud-backward`: Last released version
- `cloud-current`: Current development version (uses source)
- `cloud-forward`: Forward compatibility (uses source)
- `sm-lts`: Self-managed LTS version
- Or any direct version like `v0.147.18`

After starting services, run console tests from the console repo, then clean up:
```bash
cd ../../../console
yarn test:sql
cd -
./mzcompose down -v
```

### `list-versions` - Show Version Matrix

Displays the version matrix as JSON:

```bash
./mzcompose run list-versions
```

**Example output:**
```json
{
  "cloud-backward": "v0.147.18",
  "cloud-current": null,
  "cloud-forward": null,
  "sm-lts": "v25.2.3"
}
```

## Multi-Version Testing

To test console SQL against multiple Materialize versions, you need to implement a script **in the console repository** that:

1. Calls `./mzcompose run list-versions` to get available versions
2. For each version:
   - Calls `./mzcompose run start-version <version>`
   - Runs `yarn test:sql` from console repo
   - Calls `./mzcompose down -v` to clean up
3. Reports results

See `CONSOLE_REPO_INTEGRATION.md` for detailed implementation guide with example scripts.

## Helper Scripts

### `resolve_version.py`

Resolves version aliases to actual version tags:

```bash
./resolve_version.py cloud-backward
# Output: v0.147.18

./resolve_version.py sm-lts
# Output: v25.2.3
```

