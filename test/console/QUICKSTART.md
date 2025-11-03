# Quick Start: What You Can Run Now

## In Materialize Repo

All commands should be run from: `/Users/qindeel/dev/materialize/test/console`

### 1. List Available Workflows

```bash
./mzcompose list-workflows
```

**Output:**
```
default
list-versions
start-version
```

### 2. Get Version Matrix (JSON)

```bash
./mzcompose run list-versions
```

**Expected Output:**
```json
{
  "cloud-backward": "v0.162.2",
  "cloud-current": "v0.162.3",
  "cloud-forward": "v0.163.0",
  "sm-lts": "v0.147.18"
}
```

**Use Case:** Console repo can parse this JSON to know which versions to test.

### 3. Start Services with Default (Current Source)

```bash
./mzcompose down -v  # Clean up first
./mzcompose run default
```

**What It Does:**
- Starts Materialize with current source build
- Starts Redpanda, Postgres, MySQL, Testdrive
- Services keep running until you stop them

**Expected Output:**
```
Starting redpanda...
Starting postgres...
Starting mysql...
Starting materialized...
[Services running]
```

**After This:** You can run console tests:
```bash
cd ../../../console
yarn test:sql
```

**Clean Up:**
```bash
cd ../materialize/test/console
./mzcompose down -v
```

### 4. Start Services with Specific Version

```bash
./mzcompose down -v  # Clean up first
./mzcompose run start-version cloud-backward
# Or
./mzcompose run start-version sm-lts
# Or with direct version
./mzcompose run start-version v0.147.18
```

**What It Does:**
- Starts Materialize with the specified version (from Docker Hub)
- Starts supporting services
- Services keep running

**Expected Output:**
```
Starting services for version: cloud-backward
Docker image: materialize/materialized:v0.162.2
[Docker pulling image if needed]
[Services starting]

✅ Services started successfully
You can now run console SQL tests from the console repo:
  cd ../../../console && yarn test:sql
```

**Note:** Older versions might have issues. If a version fails to start, try a different one.

**After This:** Run console tests then clean up:
```bash
cd ../../../console
yarn test:sql
cd -
./mzcompose down -v
```

### 5. Check Running Services

```bash
docker ps
```

**Expected Output:**
```
CONTAINER ID   IMAGE                        PORTS
abc123...      materialize/materialized     0.0.0.0:6875->6875/tcp
def456...      redpanda                     0.0.0.0:9092->9092/tcp
...
```

### 6. Check Logs if Services Fail

```bash
docker logs console-materialized-1
```

### 7. Stop All Services

```bash
./mzcompose down -v
```

**What It Does:**
- Stops all containers
- Removes containers
- Removes volumes (-v flag)

## Complete Testing Flow (Manual)

Here's the complete flow to test console against a specific version:

```bash
# 1. Start services with desired version
cd /Users/qindeel/dev/materialize/test/console
./mzcompose down -v
./mzcompose run start-version cloud-backward

# 2. Run console tests
cd /Users/qindeel/dev/console
yarn test:sql

# 3. Clean up
cd /Users/qindeel/dev/materialize/test/console
./mzcompose down -v
```

## What You Need to Build in Console Repo

The console repo needs a script that automates the above flow for multiple versions:

```bash
# Pseudocode for what console repo should do
for version in (cloud-backward, cloud-current, cloud-forward, sm-lts):
    # Start services
    cd ../materialize/test/console
    ./mzcompose run start-version $version
    
    # Run tests
    cd -
    yarn test:sql
    
    # Clean up
    cd ../materialize/test/console
    ./mzcompose down -v
```

See **CONSOLE_REPO_INTEGRATION.md** for complete implementation examples in:
- Shell script
- Node.js/TypeScript
- GitHub Actions

## Troubleshooting

### Services Won't Start

**Problem:** Container exits immediately

**Check logs:**
```bash
docker logs console-materialized-1
```

**Common causes:**
- Old version doesn't exist on Docker Hub
- Version incompatible with your system
- Port already in use

**Solution:**
- Try a different version
- Use `cloud-current` (source build) which always works
- Check ports: `lsof -i :6875`

### "Module not found" Errors

Some helper scripts need to be run in the right context. Use the mzcompose workflows instead:

**Don't:**
```bash
./resolve_version.py cloud-backward  # Might fail
```

**Do:**
```bash
./mzcompose run list-versions  # Always works
```

### Console Can't Connect

**Problem:** yarn test:sql fails with connection errors

**Check services are running:**
```bash
docker ps | grep console
```

**Test connection:**
```bash
docker exec console-materialized-1 psql -h localhost -p 6875 -U materialize -c "SELECT 1"
```

**Solution:**
- Ensure services fully started (wait a few seconds)
- Check Materialize is healthy in docker ps
- Verify port 6875 is accessible

## Summary

**You Can Run Now:**
1. ✅ `./mzcompose run list-versions` - See available versions
2. ✅ `./mzcompose run default` - Start with current source
3. ✅ `./mzcompose run start-version <alias>` - Start with specific version

**You Need to Build in Console Repo:**
1. ❌ Script to iterate through versions
2. ❌ Script to run yarn test:sql for each version
3. ❌ Script to collect and report results

**Next Steps:**
1. Test the workflows manually (run the commands above)
2. Read **CONSOLE_REPO_INTEGRATION.md** for implementation guide
3. Choose implementation approach (shell/Node.js/GitHub Actions)
4. Build the test runner in console repo
5. Test locally before deploying to CI

