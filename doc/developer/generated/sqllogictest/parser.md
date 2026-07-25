---
source: src/sqllogictest/src/parser.rs
revision: fc9d219c84
---

# sqllogictest::parser

Implements `Parser<'a>`, a line-oriented parser that converts raw sqllogictest file contents into an iterator of `Record` values defined in `ast`.
Tracks current file name and line number for error reporting via `Location`.
Supports both Standard and Cockroach dialect modes, switching when it encounters a `mode cockroach` directive.
Parses `user <name>` directives into `Record::User` to switch the active session user.
Parses `let $var` directives by skipping the directive and its following query block; variables referenced in later records fail at execution time rather than parse time.
Recognizes `skip_on_retry`, `retry`, and `noticetrace` query options: `retry` queries are run once like any other; `noticetrace` queries are skipped entirely because notices are not observable.
Accepts `statement notice <regex>` (treated as plain success), and a lenient `ok`-prefix match for statement dispositions to accommodate CockroachDB's typos (`oK`, `ok;`, `oko`).
Parses `replace <regex>  <replacement>` directives (regex and replacement separated by two-or-more spaces) into `Record::Replace`; the regex is validated at parse time so errors are reported with file/line context.
