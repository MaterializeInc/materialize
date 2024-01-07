# Red-Green Parser

## The Problem

Our current parser can only produce an error or a full AST with lost token position and trivia (comment, whitespace) information.
This forced full parsing and lost information means we cannot:

1. Return position information in error messages after parsing, for example catalog name resolution.
2. Maintain comments and some newlines during pretty printing.
3. Have good auto completion points for our LSP server.

## Success Criteria

1. The planner is able to access node positions when inspecting the AST so that error messages contain position information when possible.
2. Pretty printing is able to maintain comments and newlines.
3. The LSP server is able to inspect partially valid statements.
4. This can be implemented incrementally with different parts of the parser opting in to the solution proposal: it must not require a single, large parser rewrite.

## Out of Scope

The success criteria all contain the text "is able to".
The scope of this document is to describe the work that would make it possible to implement these features, not actually implement them yet.
This is intended to scope this work down as much as possible and make it incremental.

## Solution Proposal

Enhance our parser to, in addition to the existing AST (`Statement` enum), also produce a concrete syntax tree (CST) from which AST properties can be dynamically computed by methods.
The specific technique is [red-green trees](https://ericlippert.com/2012/06/08/red-green-trees/).
These are lossless syntax trees that have various nice properties.
rust-analyzer [copied that technique](https://github.com/rust-lang/rust-analyzer/blob/master/docs/dev/syntax.md) and maintains a [`rowan` crate](https://github.com/rust-analyzer/rowan) that is a generic implementation of red-green trees.

The lexer will be taught about whitespace and comment tokens.
This will make it possible for the lexer, and later the parser, to produce the same original text (modulo keyword capitalization).

The parser will be changed to produce a CST instead of an AST.
The AST will be changed to be methods on the CST, so will not exist as an in-memory data structure.
The CST will be represented by nodes and tokens.
Nodes are conceptual parts of the syntax tree, things like `DROP_STATEMENT` and `TABLE_ALIAS`.
Tokens are leaf nodes of the syntax tree and represent the underlying SQL statement text and its position.
Errors in parsing will be their own CST node (of type error), to allow for future continuation of parsing on errors.
(This means that *all* input text will always produce a CST, it just might have an error node in it; we can still easily return these a parse errors if desired by crawling the tree to see if any are present).

For example, the statement:

```
drop VIEW blah
-- drop dependents
CASCADE;
```

will be parsed to the CST:

```
DROP_OBJECTS@0..49
  KEYWORD@0..4 "DROP"
  OBJECT_TYPE@4..9
    WHITESPACE@4..5 " "
    KEYWORD@5..9 "VIEW"
  WHITESPACE@9..10 " "
  IDENT@10..14 "blah"
  CASCADE_OR_RESTRICT@14..49
    WHITESPACE@14..19 "\n    "
    LINECOMMENT@19..37 "-- drop dependents"
    WHITESPACE@37..42 "\n    "
    KEYWORD@42..49 "CASCADE"
```

Note that each node or token is aware of its start and end position, and that whitespace and comments are fully preserved.

The AST properties will be methods on CST nodes.
The planner will call those methods instead of accessing properties on the AST structs.

### Implementation Plan

Our parser is around 8,000 lines, and another 8,000 for the AST data structures.
Doing a single-PR change across 16,000 lines is unwise.
Furthermore, this work likely does not have enough user impact to warrant such a dedicated amount of time.
Instead, the parser and AST will be kept as-is, with hooks for this work added in over time.
The parser will produce the existing AST, and also a new CST structure of varying completeness.
We can slowly teach the parser how to correctly parse all statement kinds, along with teaching a new AST module how to examine the CST and emit AST information.
When certain statements are fully supported by both the parser and AST module, the planner will be switched to use those.
For statements that have not been taught about their CST, the produced CST will be flat (i.e., a single list of tokens) which is unsuitable for AST use.

This approach should allow for incrementally supporting all statements with the minimum possible initial time commitment, and also for implementing some of the success criteria goals.
For example, we could have a pretty printer that works on the CST.
Initially it won't pretty print very well, but as more statements are taught to make correct CSTs, pretty printing won't have to be changed at all because it works generically over any CST.

## Minimal Viable Prototype

An MVP demonstrating all of this is available at https://github.com/mjibson/materialize/tree/bursera

It:

- implements a progressive parser that produces correct CSTs for `DROP`, and partial CSTs for `SELECT`.
- implements a pretty printer over CSTs
- implements a catalog name resolver for some object types and enhances error messages with locations of unresolvable objects

## Alternatives

Go's standard library contains some packages for parsing and formatting Go code.
These are sufficient to compile Go code and also format it, so it must keep around the comments somehow.
That method is a [CommentMap](https://pkg.go.dev/go/ast#CommentMap), which is a map from `Node` to some comments.
A `Node` is an interface, which in Go is a pointer.
Thus, Go is able to separate trivia (comments and whitespace) from non-trivia by storing them separately.
Could we use a similar technique: storing pointers to the various Statement sub-structs?
This seems like a clear no because we clone Statements all the time, presumably creating new pointer locations, which seems difficult to propogate to some comment or token-position map.

## Open questions

- Performance and memory benchmarking to make sure we don't regress on either unacceptably.
- Ease of use for non-adapter teams.
  Other teams must be able to modify the parser and AST without being experts in this method.
  Splitting up parsing and AST methods may end up being overly complicated.
