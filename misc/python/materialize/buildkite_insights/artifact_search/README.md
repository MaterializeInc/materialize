# Buildkite Artifact Search

This tool allows searching Buildkite artifacts (log and text files, including zst-compressed ones) of a build.
zst-compressed files are included by default.

## Usage
```
usage: buildkite-artifact-search [-h]
                                 [--fetch {auto,always,avoid,never}]
                                 [--include-zst-files | --no-include-zst-files]
                                 [--jobid JOBID]
                                 [--max-results MAX_RESULTS]
                                 [--use-regex]
                                 {cleanup,coverage,deploy,deploy-mz-lsp-server,deploy-mz,deploy-website,license,nightly,release-qualification,security,slt,test,www}
                                 buildnumber
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
