An example of python-based workflows that does not actually test anything.

If the directory contains a file named mzworkflows.py, all functions
from it that start with 'workflow_' will be made available for execution
as if they are regular mzcompose.yml workflows.

A Python workflow can call other Steps as methods to the Workflow,
as shown in the mzworkflows.py example in this directory.
