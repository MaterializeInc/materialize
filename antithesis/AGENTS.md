This directory contains files relevant to running tests in Antithesis.

Use the `antithesis-setup` skill to scaffold and manage this directory. Use the `antithesis-research` skill to analyze the system and build a property catalog. Use the `antithesis-workload` skill to implement assertions and test commands.

**setup-complete.sh**
Inject this script into a Dockerfile to notify Antithesis that setup is complete. This script should only run once the system under test is ready for testing. Antithesis will not run any test commands until it receives this event.

**config**
This directory contains the `docker-compose.yaml` file used to bring up this system within the Antithesis environment, along with any closely related config files.

**scratchbook**
This directory is the Antithesis scratchbook for the codebase. It contains documents such as system analysis, property catalogs, topology plans, per-property evidence files (in `scratchbook/properties/`), property relationship maps, and other persistent integration notes. Keep it up to date as Antithesis-related decisions change.

**test**
This directory contains test templates. A test template is a directory containing test command executable files. Each test command must have a valid prefix: `parallel_driver_, singleton_driver_, serial_driver_, first_, eventually_, finally_, anytime_`. Prefixes constrain when and how commands are composed in a single timeline. Files or subdirectories prefixed with `helper_` are ignored by Test Composer and can be used for helper scripts kept alongside the commands.
