# Buildkite Artifact Search

This tool allows searching Buildkite artifacts (log and text files, including zst-compressed ones) of a build.
zst-compressed files are included by default.

## Usage
```
usage: buildkite-artifact-search [-h]
                                 [--fetch {auto,always,avoid,never}]
                                 [--file-name-regex FILE_NAME_REGEX]
                                 [--include-zst-files | --no-include-zst-files]
                                 [--job-id JOB_ID]
                                 [--max-results MAX_RESULTS]
                                 [--search-logs-instead-of-artifacts]
                                 [--use-regex]
                                 {cleanup,coverage,deploy,deploy-mz-lsp-server,deploy-mz,deploy-website,license,nightly,qa-canary,release-qualification,security,slt,test,www}
                                 buildnumber
                                 pattern
```

### Authentication

You will need an environment variable called `BUILDKITE_TOKEN`, which contains a Buildkite token. Such a token can be
created on https://buildkite.com/user/api-access-tokens/new.
This tool will need:
* `read_artifacts`

## Examples

Search artifacts in nightly build materialize#7639 for the string `mytable0`

```
bin/buildkite-artifact-search nightly 7639 "mytable0"
```

Search artifacts of the job execution `018f4888-2d3f-494c-8ea0-6ab854a6b1f1` in nightly build materialize#7639 for the string `mytable0`

```
bin/buildkite-artifact-search nightly 7639 "mytable0" --job-id "018f4888-2d3f-494c-8ea0-6ab854a6b1f1"
```
