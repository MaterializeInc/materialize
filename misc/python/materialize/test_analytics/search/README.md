# Test Analytics Annotation Search

This tool allows searching annotations uploaded to the test-analytics database.

## Usage
```
usage: test-analytics-annotation-search [-h]
                                        --test-analytics-hostname "${TEST_ANALYTICS_HOSTNAME}"
                                        --test-analytics-username "${TEST_ANALYTICS_USERNAME}"
                                        --test-analytics-app-password "${TEST_ANALYTICS_APP_PASSWORD}"
                                        [--branch BRANCH]
                                        [--build-step-key BUILD_STEP_KEY]
                                        [--max-results MAX_RESULTS]
                                        [--not-newer-than-build-number NOT_NEWER_THAN_BUILD_NUMBER]
                                        [--oneline]
                                        [--only-failed-builds]
                                        [--short]
                                        {cleanup,coverage,deploy,deploy-mz-lsp-server,deploy-mz,deploy-website,license,nightly,qa-canary,release-qualification,security,slt,test,www,*}
                                        pattern
```

### Connection & Authentication

Required environment variables:
* `TEST_ANALYTICS_HOSTNAME` (currently: `7vifiksqeftxc6ld3r6zvc8n2.lb.us-east-1.aws.materialize.cloud`)
* `TEST_ANALYTICS_USERNAME` (your username)
* `TEST_ANALYTICS_APP_PASSWORD` (an app password created in the
 [Materialize Cloud console](https://console.materialize.com/access) for the "Materialize Production Analytics" organization)

## Examples

Builds in nightly pipeline that contain the string `regression`

```
bin/test-analytics-search test "regression"
```

Older builds of the main branch in nightly pipeline that contain the string `regression`

```
bin/test-analytics-search test "regression" --not-newer-than-build-number 23000 --branch main
```

Builds in test pipelines that contain the string `panicked` followed by `stack.rs` in annotations of the `testdrive` build step (of any shard)
```
bin/test-analytics-search test "panicked%stack.rs" --build-step-key "testdrive"
```

Builds in all pipelines that contain the string `regression` and failed
```
bin/test-analytics-search "*" "regression" --only-failed-builds
```
