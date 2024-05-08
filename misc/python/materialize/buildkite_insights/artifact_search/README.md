# Buildkite Artifact Search

This tool allows searching Buildkite artifacts of a build.
Note that some files are omitted (e.g., compressed files).

## Usage
```
usage: buildkite-artifact-search [-h]
                                 [--max-results MAX_RESULTS]
                                 [--use-regex]
                                 [--fetch {auto,always,never}]
                                 {cleanup,coverage,deploy,deploy-mz-lsp-server,deploy-mz,deploy-website,license,nightly,release-qualification,security,slt,test,www}
                                 buildnumber
                                 jobid
                                 pattern
```

### Authentication

You will need an environment variable called `BUILDKITE_TOKEN`, which contains a Buildkite token. Such a token can be
created on https://buildkite.com/user/api-access-tokens/new.
This tool will need:
* `read_artifacts`

## Examples

Search artifacts of the job execution `018f4888-2d3f-494c-8ea0-6ab854a6b1f1` in nightly build #7639 for the string `mytable0`

```
bin/buildkite-artifact-search nightly 7639 "018f4888-2d3f-494c-8ea0-6ab854a6b1f1" "mytable0"
```
