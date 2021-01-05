# testdrive/esoteric

Tests in this directory depend on esoteric infrastructure or configuration
that most developers do not have installed on their machine. The goal is to
keep these tests from matching the standard `test/testdrive/*.td`, which
most developers use to select testdrive tests locally. CI, however, will
automatically run all tests in this directory.
