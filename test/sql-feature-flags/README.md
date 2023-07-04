# Introduction

The purpose of this folder is to test various boolean features that enable SQL syntax.
The tested contract is that:

1. Disabled SQL syntax should trigger an error if the SQL feature is disabled.
1. Disabled SQL syntax should not trigger an error if the SQL feature is enabled.
1. Disabled SQL syntax should not trigger an error on catalog rehydration even if the feature is disabled.

# Running

```bash
./mzcompose down -v
./mzcompose run default
```

# Adding new content

Subclass the `FeatureTestScenario` class.
