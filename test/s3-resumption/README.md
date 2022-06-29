An end-to-end test for the S3 resumption logic.

Toxiproxy is used to interrupt the TCP connection between Mz and S3/SQS
at different stages of processing.

By regulating the number of bytes that are allowed to go through, mzcompose.py
can use the same sequence of .td steps but inject the a failure that
impacts a different operation. A smaller limit does not allow much to happen,
wheres a larger limit only prevents the actual S3 value from being read,
but allows for S3 bucket listing and SQS operations.
