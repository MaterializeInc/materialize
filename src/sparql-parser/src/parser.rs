// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SPARQL 1.1 recursive-descent parser.
//!
//! This module provides the entry point [`parse`] which takes a SPARQL query
//! string and produces a [`SparqlQuery`] AST node. The parser consumes
//! tokens produced by [`crate::lexer::lex`].
//!
//! The parser is built incrementally:
//! - Prompt 1: skeleton + entry point
//! - Prompt 2 (this): basic SELECT queries (BGP + FILTER)
//! - Prompt 3: OPTIONAL, UNION, MINUS, BIND, VALUES
//! - Prompt 4: CONSTRUCT, ASK, DESCRIBE
//! - Prompt 5: property paths
//! - Prompt 6: aggregates, subqueries, GRAPH, solution modifiers

use std::collections::HashMap;
use std::fmt;

use crate::ast::*;
use crate::lexer::{self, Keyword, LexerError, PosToken, Token};

/// An error encountered during parsing.
#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    pub message: String,
    pub pos: usize,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "parse error at byte {}: {}", self.pos, self.message)
    }
}

impl std::error::Error for ParseError {}

impl From<LexerError> for ParseError {
    fn from(e: LexerError) -> Self {
        ParseError {
            message: e.message,
            pos: e.pos,
        }
    }
}

/// Parse a SPARQL query string into an AST.
pub fn parse(input: &str) -> Result<SparqlQuery, ParseError> {
    let tokens = lexer::lex(input)?;
    let mut parser = Parser::new(input, tokens);
    let query = parser.parse_query()?;
    if parser.peek().is_some() {
        return Err(parser.expected("end of query"));
    }
    Ok(query)
}

/// The parser state.
pub(crate) struct Parser<'a> {
    #[allow(dead_code)]
    input: &'a str,
    tokens: Vec<PosToken>,
    pos: usize,
    /// Prefix map built from PREFIX declarations, used to resolve prefixed names.
    prefixes: HashMap<String, String>,
    /// Base IRI from BASE declaration, used to resolve relative IRIs.
    #[allow(dead_code)]
    base: Option<String>,
}

impl<'a> Parser<'a> {
    pub(crate) fn new(input: &'a str, tokens: Vec<PosToken>) -> Self {
        Parser {
            input,
            tokens,
            pos: 0,
            prefixes: HashMap::new(),
            base: None,
        }
    }

    // === Core helpers ===

    /// Peek at the current token without consuming it.
    pub(crate) fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos).map(|t| &t.kind)
    }

    /// Consume and return a clone of the current token.
    fn next_token(&mut self) -> Option<Token> {
        if self.pos < self.tokens.len() {
            let token = self.tokens[self.pos].kind.clone();
            self.pos += 1;
            Some(token)
        } else {
            None
        }
    }

    /// Advance past the current token without returning it.
    fn bump(&mut self) {
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
    }

    /// Get the byte offset of the current token (or end of input).
    pub(crate) fn current_pos(&self) -> usize {
        self.tokens
            .get(self.pos)
            .map(|t| t.offset)
            .unwrap_or(self.input.len())
    }

    /// Return a parse error at the current position.
    pub(crate) fn error(&self, message: impl Into<String>) -> ParseError {
        ParseError {
            message: message.into(),
            pos: self.current_pos(),
        }
    }

    /// Return a parse error indicating an unexpected token.
    pub(crate) fn expected(&self, expected: &str) -> ParseError {
        match self.peek() {
            Some(tok) => self.error(format!("expected {}, found {:?}", expected, tok)),
            None => self.error(format!("expected {}, found end of input", expected)),
        }
    }

    /// Consume the current token if it matches the given keyword.
    fn eat_keyword(&mut self, kw: Keyword) -> bool {
        if matches!(self.peek(), Some(Token::Keyword(k)) if *k == kw) {
            self.bump();
            true
        } else {
            false
        }
    }

    /// Consume the current token if it matches the given keyword, or return an error.
    fn expect_keyword(&mut self, kw: Keyword) -> Result<(), ParseError> {
        if self.eat_keyword(kw) {
            Ok(())
        } else {
            Err(self.expected(&format!("{}", kw)))
        }
    }

    /// Consume the current token if it matches exactly.
    fn eat(&mut self, expected: &Token) -> bool {
        if self.peek() == Some(expected) {
            self.bump();
            true
        } else {
            false
        }
    }

    /// Consume the current token if it matches, or return an error.
    fn expect(&mut self, expected: &Token, desc: &str) -> Result<(), ParseError> {
        if self.eat(expected) {
            Ok(())
        } else {
            Err(self.expected(desc))
        }
    }

    // === Query parsing ===

    /// Parse a complete SPARQL query.
    pub(crate) fn parse_query(&mut self) -> Result<SparqlQuery, ParseError> {
        // Prologue: BASE and PREFIX declarations
        let base = self.parse_base_decl()?;
        let prefixes = self.parse_prefix_decls()?;

        // Query form dispatch
        match self.peek() {
            Some(Token::Keyword(Keyword::Select)) => {
                let form = self.parse_select_clause()?;
                let where_clause = self.parse_where_clause()?;
                // Solution modifiers — not yet supported, will be added in prompt 6
                Ok(SparqlQuery {
                    base,
                    prefixes,
                    form,
                    where_clause,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                })
            }
            Some(Token::Keyword(Keyword::Construct)) => {
                self.bump();
                // Two forms: CONSTRUCT { template } WHERE { pattern }
                //        or: CONSTRUCT WHERE { pattern } (short form: template = pattern)
                if matches!(self.peek(), Some(Token::Keyword(Keyword::Where))) {
                    // Short form: CONSTRUCT WHERE { pattern }
                    let where_clause = self.parse_where_clause()?;
                    let template = self.extract_triples_from_pattern(&where_clause);
                    Ok(SparqlQuery {
                        base,
                        prefixes,
                        form: QueryForm::Construct { template },
                        where_clause,
                        group_by: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                        offset: None,
                    })
                } else {
                    // Full form: CONSTRUCT { template } WHERE { pattern }
                    let template = self.parse_construct_template()?;
                    let where_clause = self.parse_where_clause()?;
                    // Solution modifiers — not yet supported, will be added in prompt 6
                    Ok(SparqlQuery {
                        base,
                        prefixes,
                        form: QueryForm::Construct { template },
                        where_clause,
                        group_by: vec![],
                        having: None,
                        order_by: vec![],
                        limit: None,
                        offset: None,
                    })
                }
            }
            Some(Token::Keyword(Keyword::Ask)) => {
                self.bump();
                let where_clause = self.parse_where_clause()?;
                Ok(SparqlQuery {
                    base,
                    prefixes,
                    form: QueryForm::Ask,
                    where_clause,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                })
            }
            Some(Token::Keyword(Keyword::Describe)) => {
                self.bump();
                let resources = self.parse_describe_resources()?;
                // WHERE clause is optional for DESCRIBE
                let where_clause = if matches!(
                    self.peek(),
                    Some(Token::Keyword(Keyword::Where)) | Some(Token::LBrace)
                ) {
                    self.parse_where_clause()?
                } else {
                    GroupGraphPattern::Basic(vec![])
                };
                Ok(SparqlQuery {
                    base,
                    prefixes,
                    form: QueryForm::Describe { resources },
                    where_clause,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                })
            }
            _ => Err(self.expected("SELECT, CONSTRUCT, ASK, or DESCRIBE")),
        }
    }

    fn parse_base_decl(&mut self) -> Result<Option<Iri>, ParseError> {
        if self.eat_keyword(Keyword::Base) {
            let iri = self.parse_full_iri()?;
            self.base = Some(iri.value.clone());
            Ok(Some(iri))
        } else {
            Ok(None)
        }
    }

    fn parse_prefix_decls(&mut self) -> Result<Vec<PrefixDecl>, ParseError> {
        let mut decls = Vec::new();
        while self.eat_keyword(Keyword::Prefix) {
            // Expect a prefixed name token with empty local part (e.g., `ex:`)
            let prefix = match self.next_token() {
                Some(Token::PrefixedName { prefix, local }) => {
                    if !local.is_empty() {
                        return Err(self.error(format!(
                            "expected prefix declaration like '{}:', found '{}:{}'",
                            prefix, prefix, local
                        )));
                    }
                    prefix
                }
                _ => return Err(self.expected("prefix name (e.g., 'ex:')")),
            };
            let iri = self.parse_full_iri()?;
            self.prefixes.insert(prefix.clone(), iri.value.clone());
            decls.push(PrefixDecl { prefix, iri });
        }
        Ok(decls)
    }

    fn parse_select_clause(&mut self) -> Result<QueryForm, ParseError> {
        self.expect_keyword(Keyword::Select)?;

        // Optional DISTINCT / REDUCED
        let modifier = if self.eat_keyword(Keyword::Distinct) {
            SelectModifier::Distinct
        } else if self.eat_keyword(Keyword::Reduced) {
            SelectModifier::Reduced
        } else {
            SelectModifier::Default
        };

        // Projection: `*` or variable/expression list
        let projection = if self.eat(&Token::Star) {
            SelectClause::Wildcard
        } else {
            let mut vars = Vec::new();
            loop {
                match self.peek() {
                    Some(Token::Variable(_)) => {
                        let var = self.parse_variable()?;
                        vars.push(SelectVariable::Variable(var));
                    }
                    Some(Token::LParen) => {
                        // (expression AS ?var)
                        self.bump();
                        let expr = self.parse_expression()?;
                        self.expect_keyword(Keyword::As)?;
                        let var = self.parse_variable()?;
                        self.expect(&Token::RParen, "')'")?;
                        vars.push(SelectVariable::Expression(expr, var));
                    }
                    _ => break,
                }
            }
            if vars.is_empty() {
                return Err(self.expected("variable or '*' in SELECT clause"));
            }
            SelectClause::Variables(vars)
        };

        Ok(QueryForm::Select {
            modifier,
            projection,
        })
    }

    fn parse_where_clause(&mut self) -> Result<GroupGraphPattern, ParseError> {
        // The WHERE keyword is optional in SPARQL
        self.eat_keyword(Keyword::Where);
        self.parse_group_graph_pattern()
    }

    /// Parse a CONSTRUCT template: `{ TriplesSameSubject ( '.' TriplesSameSubject )* '.'? }`.
    ///
    /// Similar to a triples block but enclosed in braces and without non-triple
    /// patterns (no FILTER, OPTIONAL, etc.).
    fn parse_construct_template(&mut self) -> Result<Vec<TriplePattern>, ParseError> {
        self.expect(&Token::LBrace, "'{'")?;
        let mut triples = Vec::new();
        if self.can_start_triple() {
            triples = self.parse_triples_block()?;
        }
        self.expect(&Token::RBrace, "'}'")?;
        Ok(triples)
    }

    /// Parse the resource list for DESCRIBE: `'*' | (VarOrIri)+`.
    fn parse_describe_resources(&mut self) -> Result<Vec<VarOrIri>, ParseError> {
        if self.eat(&Token::Star) {
            // DESCRIBE * — empty resources list signals "all"
            // We represent this as an empty vec; the planner interprets it as
            // "describe all resources from WHERE clause".
            return Ok(vec![]);
        }
        let mut resources = Vec::new();
        loop {
            match self.peek() {
                Some(Token::Variable(_)) => {
                    let var = self.parse_variable()?;
                    resources.push(VarOrIri::Variable(var));
                }
                Some(Token::Iri(_)) | Some(Token::PrefixedName { .. }) => {
                    let iri = self.parse_iri()?;
                    resources.push(VarOrIri::Iri(iri));
                }
                _ => break,
            }
        }
        if resources.is_empty() {
            return Err(self.expected("variable, IRI, or '*' after DESCRIBE"));
        }
        Ok(resources)
    }

    /// Extract triple patterns from a group graph pattern (used for
    /// CONSTRUCT WHERE short form where the template equals the WHERE pattern).
    fn extract_triples_from_pattern(&self, pattern: &GroupGraphPattern) -> Vec<TriplePattern> {
        match pattern {
            GroupGraphPattern::Basic(triples) => triples.clone(),
            GroupGraphPattern::Group(patterns) => patterns
                .iter()
                .flat_map(|p| self.extract_triples_from_pattern(p))
                .collect(),
            _ => vec![],
        }
    }

    // === Graph pattern parsing ===

    fn parse_group_graph_pattern(&mut self) -> Result<GroupGraphPattern, ParseError> {
        self.expect(&Token::LBrace, "'{'")?;
        let pattern = self.parse_group_graph_pattern_sub()?;
        self.expect(&Token::RBrace, "'}'")?;
        Ok(pattern)
    }

    /// Parse the contents of a `{ ... }` group graph pattern.
    ///
    /// Grammar: `TriplesBlock? (GraphPatternNotTriples '.'? TriplesBlock?)*`
    ///
    /// The SPARQL grammar interleaves triple patterns and non-triple patterns
    /// (OPTIONAL, UNION, MINUS, BIND, VALUES, FILTER, nested groups). UNION
    /// is special: it chains onto the *preceding* group pattern, so
    /// `{ P1 } UNION { P2 } UNION { P3 }` is parsed as a single UNION node.
    fn parse_group_graph_pattern_sub(&mut self) -> Result<GroupGraphPattern, ParseError> {
        let mut patterns: Vec<GroupGraphPattern> = Vec::new();
        let mut triples: Vec<TriplePattern> = Vec::new();

        /// Flush accumulated triples into a Basic pattern and push onto patterns.
        fn flush_triples(triples: &mut Vec<TriplePattern>, patterns: &mut Vec<GroupGraphPattern>) {
            if !triples.is_empty() {
                patterns.push(GroupGraphPattern::Basic(std::mem::take(triples)));
            }
        }

        loop {
            // Try to parse a triples block
            if self.can_start_triple() {
                let mut block = self.parse_triples_block()?;
                triples.append(&mut block);
            }

            // Check for graph pattern non-triples
            match self.peek() {
                Some(Token::Keyword(Keyword::Filter)) => {
                    flush_triples(&mut triples, &mut patterns);
                    self.bump(); // consume FILTER
                    let expr = self.parse_constraint()?;
                    patterns.push(GroupGraphPattern::Filter(expr));
                    self.eat(&Token::Dot);
                }
                Some(Token::Keyword(Keyword::Optional)) => {
                    flush_triples(&mut triples, &mut patterns);
                    self.bump(); // consume OPTIONAL
                    let inner = self.parse_group_graph_pattern()?;
                    patterns.push(GroupGraphPattern::Optional(Box::new(inner)));
                    self.eat(&Token::Dot);
                }
                Some(Token::Keyword(Keyword::Minus)) => {
                    flush_triples(&mut triples, &mut patterns);
                    self.bump(); // consume MINUS
                    let inner = self.parse_group_graph_pattern()?;
                    patterns.push(GroupGraphPattern::Minus(Box::new(inner)));
                    self.eat(&Token::Dot);
                }
                Some(Token::Keyword(Keyword::Bind)) => {
                    flush_triples(&mut triples, &mut patterns);
                    self.bump(); // consume BIND
                    self.expect(&Token::LParen, "'('")?;
                    let expr = self.parse_expression()?;
                    self.expect_keyword(Keyword::As)?;
                    let var = self.parse_variable()?;
                    self.expect(&Token::RParen, "')'")?;
                    patterns.push(GroupGraphPattern::Bind(expr, var));
                    self.eat(&Token::Dot);
                }
                Some(Token::Keyword(Keyword::Values)) => {
                    flush_triples(&mut triples, &mut patterns);
                    let values = self.parse_inline_data()?;
                    patterns.push(values);
                    self.eat(&Token::Dot);
                }
                Some(Token::Keyword(Keyword::Graph)) => {
                    return Err(self.error("GRAPH patterns not yet supported (see prompt 6)"));
                }
                Some(Token::Keyword(Keyword::Service)) => {
                    return Err(self.error("SERVICE patterns not yet supported"));
                }
                Some(Token::LBrace) => {
                    // Nested group — could be followed by UNION
                    flush_triples(&mut triples, &mut patterns);
                    let mut group = self.parse_group_graph_pattern()?;

                    // Check for UNION chains: { P1 } UNION { P2 } UNION { P3 }
                    while self.eat_keyword(Keyword::Union) {
                        let right = self.parse_group_graph_pattern()?;
                        group = GroupGraphPattern::Union(Box::new(group), Box::new(right));
                    }

                    patterns.push(group);
                    self.eat(&Token::Dot);
                }
                _ => break,
            }
        }

        // Flush remaining triples
        flush_triples(&mut triples, &mut patterns);

        // Simplify representation
        match patterns.len() {
            0 => Ok(GroupGraphPattern::Basic(vec![])),
            1 => Ok(patterns.pop().unwrap()),
            _ => Ok(GroupGraphPattern::Group(patterns)),
        }
    }

    /// Parse a VALUES inline data block.
    ///
    /// Grammar: `VALUES DataBlock`
    /// DataBlock: `InlineDataOneVar | InlineDataFull`
    /// InlineDataOneVar: `Var '{' DataBlockValue* '}'`
    /// InlineDataFull: `( Var* ) '{' ( '(' DataBlockValue* ')' )* '}'`
    fn parse_inline_data(&mut self) -> Result<GroupGraphPattern, ParseError> {
        self.expect_keyword(Keyword::Values)?;

        // Parse variable list — either a single variable or `(var1 var2 ...)`
        let variables = if self.eat(&Token::LParen) {
            let mut vars = Vec::new();
            while matches!(self.peek(), Some(Token::Variable(_))) {
                vars.push(self.parse_variable()?);
            }
            self.expect(&Token::RParen, "')'")?;
            vars
        } else if matches!(self.peek(), Some(Token::Variable(_))) {
            // Single variable (no parens)
            vec![self.parse_variable()?]
        } else {
            return Err(self.expected("variable or '(' in VALUES clause"));
        };

        let num_vars = variables.len();

        // Parse data block: `{ (val1 val2) (val3 val4) ... }` or `{ val1 val2 ... }` for single var
        self.expect(&Token::LBrace, "'{'")?;
        let mut rows = Vec::new();

        if num_vars == 1 {
            // Single variable form: VALUES ?x { 1 2 3 "hello" UNDEF }
            while !matches!(self.peek(), Some(Token::RBrace) | None) {
                if self.eat_keyword(Keyword::Undef) {
                    rows.push(vec![None]);
                } else {
                    let term = self.parse_data_block_value()?;
                    rows.push(vec![Some(term)]);
                }
            }
        } else {
            // Multi-variable form: VALUES (?x ?y) { (1 2) (3 UNDEF) }
            while self.eat(&Token::LParen) {
                let mut row = Vec::new();
                for _ in 0..num_vars {
                    if self.eat_keyword(Keyword::Undef) {
                        row.push(None);
                    } else {
                        let term = self.parse_data_block_value()?;
                        row.push(Some(term));
                    }
                }
                self.expect(&Token::RParen, "')'")?;
                rows.push(row);
            }
        }

        self.expect(&Token::RBrace, "'}'")?;

        Ok(GroupGraphPattern::Values { variables, rows })
    }

    /// Parse a single value in a VALUES data block.
    ///
    /// Valid values: IRI, literal (string, numeric, boolean), UNDEF handled by caller.
    fn parse_data_block_value(&mut self) -> Result<GraphTerm, ParseError> {
        match self.peek() {
            Some(Token::Iri(_)) | Some(Token::PrefixedName { .. }) => {
                let iri = self.parse_iri()?;
                Ok(GraphTerm::Iri(iri))
            }
            Some(Token::StringLiteral(_)) | Some(Token::LongStringLiteral(_)) => {
                let lit = self.parse_rdf_literal()?;
                Ok(GraphTerm::Literal(lit))
            }
            Some(Token::Integer(_)) | Some(Token::Decimal(_)) | Some(Token::Double(_)) => {
                if let Some(tok) = self.next_token() {
                    let s = match tok {
                        Token::Integer(s) | Token::Decimal(s) | Token::Double(s) => s,
                        _ => unreachable!(),
                    };
                    Ok(GraphTerm::NumericLiteral(s))
                } else {
                    unreachable!()
                }
            }
            Some(Token::Keyword(Keyword::True)) => {
                self.bump();
                Ok(GraphTerm::BooleanLiteral(true))
            }
            Some(Token::Keyword(Keyword::False)) => {
                self.bump();
                Ok(GraphTerm::BooleanLiteral(false))
            }
            _ => Err(self.expected("data block value (IRI, literal, or UNDEF)")),
        }
    }

    /// Check if the current token can start a triple pattern.
    fn can_start_triple(&self) -> bool {
        matches!(
            self.peek(),
            Some(Token::Variable(_))
                | Some(Token::Iri(_))
                | Some(Token::PrefixedName { .. })
                | Some(Token::BlankNodeLabel(_))
                | Some(Token::StringLiteral(_))
                | Some(Token::LongStringLiteral(_))
                | Some(Token::Integer(_))
                | Some(Token::Decimal(_))
                | Some(Token::Double(_))
                | Some(Token::Keyword(Keyword::True))
                | Some(Token::Keyword(Keyword::False))
        )
    }

    /// Parse a triples block: one or more triple-same-subject patterns
    /// separated by `.`.
    fn parse_triples_block(&mut self) -> Result<Vec<TriplePattern>, ParseError> {
        let mut triples = self.parse_triples_same_subject()?;

        while self.eat(&Token::Dot) {
            if self.can_start_triple() {
                let mut more = self.parse_triples_same_subject()?;
                triples.append(&mut more);
            }
        }

        Ok(triples)
    }

    /// Parse triples sharing the same subject, handling `;` and `,` shorthand.
    ///
    /// Grammar: `VarOrTerm PropertyListNotEmpty`
    fn parse_triples_same_subject(&mut self) -> Result<Vec<TriplePattern>, ParseError> {
        let subject = self.parse_var_or_term()?;
        self.parse_property_list(subject)
    }

    /// Parse a property list: `Verb ObjectList (';' Verb ObjectList)*`
    fn parse_property_list(
        &mut self,
        subject: VarOrTerm,
    ) -> Result<Vec<TriplePattern>, ParseError> {
        let mut triples = Vec::new();

        let predicate = self.parse_verb()?;
        let objects = self.parse_object_list()?;
        for object in objects {
            triples.push(TriplePattern {
                subject: subject.clone(),
                predicate: predicate.clone(),
                object,
            });
        }

        // Handle `;` — introduces additional predicate-object pairs for the same subject
        while self.eat(&Token::Semicolon) {
            // Trailing `;` is allowed (no predicate-object pair follows)
            if !self.can_start_verb() {
                break;
            }
            let predicate = self.parse_verb()?;
            let objects = self.parse_object_list()?;
            for object in objects {
                triples.push(TriplePattern {
                    subject: subject.clone(),
                    predicate: predicate.clone(),
                    object,
                });
            }
        }

        Ok(triples)
    }

    /// Parse an object list: `VarOrTerm (',' VarOrTerm)*`
    fn parse_object_list(&mut self) -> Result<Vec<VarOrTerm>, ParseError> {
        let mut objects = vec![self.parse_var_or_term()?];
        while self.eat(&Token::Comma) {
            objects.push(self.parse_var_or_term()?);
        }
        Ok(objects)
    }

    /// Check if the current token can start a verb (predicate).
    fn can_start_verb(&self) -> bool {
        matches!(
            self.peek(),
            Some(Token::Variable(_))
                | Some(Token::Iri(_))
                | Some(Token::PrefixedName { .. })
                | Some(Token::Keyword(Keyword::A))
        )
    }

    /// Parse a verb (predicate): variable, IRI, or `a` (rdf:type).
    ///
    /// Property paths are not yet supported (see prompt 5).
    fn parse_verb(&mut self) -> Result<VerbPath, ParseError> {
        match self.peek() {
            Some(Token::Variable(_)) => {
                let var = self.parse_variable()?;
                Ok(VerbPath::Variable(var))
            }
            Some(Token::Keyword(Keyword::A)) => {
                self.bump();
                Ok(VerbPath::Path(PropertyPath::Iri(Iri {
                    value: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_string(),
                })))
            }
            Some(Token::Iri(_)) | Some(Token::PrefixedName { .. }) => {
                let iri = self.parse_iri()?;
                Ok(VerbPath::Path(PropertyPath::Iri(iri)))
            }
            _ => Err(self.expected("predicate (variable, IRI, or 'a')")),
        }
    }

    /// Parse a variable or RDF term in subject/object position.
    fn parse_var_or_term(&mut self) -> Result<VarOrTerm, ParseError> {
        match self.peek() {
            Some(Token::Variable(_)) => {
                let var = self.parse_variable()?;
                Ok(VarOrTerm::Variable(var))
            }
            Some(Token::Iri(_)) | Some(Token::PrefixedName { .. }) => {
                let iri = self.parse_iri()?;
                Ok(VarOrTerm::Term(GraphTerm::Iri(iri)))
            }
            Some(Token::BlankNodeLabel(_)) => {
                if let Some(Token::BlankNodeLabel(label)) = self.next_token() {
                    Ok(VarOrTerm::Term(GraphTerm::BlankNode(label)))
                } else {
                    unreachable!()
                }
            }
            Some(Token::StringLiteral(_)) | Some(Token::LongStringLiteral(_)) => {
                let lit = self.parse_rdf_literal()?;
                Ok(VarOrTerm::Term(GraphTerm::Literal(lit)))
            }
            Some(Token::Integer(_)) | Some(Token::Decimal(_)) | Some(Token::Double(_)) => {
                if let Some(tok) = self.next_token() {
                    let s = match tok {
                        Token::Integer(s) | Token::Decimal(s) | Token::Double(s) => s,
                        _ => unreachable!(),
                    };
                    Ok(VarOrTerm::Term(GraphTerm::NumericLiteral(s)))
                } else {
                    unreachable!()
                }
            }
            Some(Token::Keyword(Keyword::True)) => {
                self.bump();
                Ok(VarOrTerm::Term(GraphTerm::BooleanLiteral(true)))
            }
            Some(Token::Keyword(Keyword::False)) => {
                self.bump();
                Ok(VarOrTerm::Term(GraphTerm::BooleanLiteral(false)))
            }
            _ => Err(self.expected("variable or RDF term")),
        }
    }

    /// Parse a SPARQL variable (`?name` or `$name`).
    fn parse_variable(&mut self) -> Result<Variable, ParseError> {
        match self.next_token() {
            Some(Token::Variable(name)) => Ok(Variable { name }),
            _ => Err(self.expected("variable")),
        }
    }

    /// Parse an RDF literal with optional language tag or datatype.
    fn parse_rdf_literal(&mut self) -> Result<RdfLiteral, ParseError> {
        let value = match self.next_token() {
            Some(Token::StringLiteral(s)) | Some(Token::LongStringLiteral(s)) => s,
            _ => return Err(self.expected("string literal")),
        };

        // Check for language tag (@en) or datatype (^^<iri>)
        match self.peek() {
            Some(Token::LangTag(_)) => {
                if let Some(Token::LangTag(lang)) = self.next_token() {
                    Ok(RdfLiteral::LanguageTagged {
                        value,
                        language: lang,
                    })
                } else {
                    unreachable!()
                }
            }
            Some(Token::DoubleCaret) => {
                self.bump();
                let datatype = self.parse_iri()?;
                Ok(RdfLiteral::Typed { value, datatype })
            }
            _ => Ok(RdfLiteral::Simple(value)),
        }
    }

    /// Parse an IRI — either a full IRI (`<...>`) or a prefixed name (`ex:foo`).
    fn parse_iri(&mut self) -> Result<Iri, ParseError> {
        match self.next_token() {
            Some(Token::Iri(value)) => Ok(Iri { value }),
            Some(Token::PrefixedName { prefix, local }) => {
                self.resolve_prefixed_name(&prefix, &local)
            }
            _ => Err(self.expected("IRI")),
        }
    }

    /// Parse a full IRI (only `<...>` form, not prefixed names).
    fn parse_full_iri(&mut self) -> Result<Iri, ParseError> {
        match self.next_token() {
            Some(Token::Iri(value)) => Ok(Iri { value }),
            _ => Err(self.expected("full IRI (<...>)")),
        }
    }

    /// Resolve a prefixed name to a full IRI using the PREFIX declarations.
    fn resolve_prefixed_name(&self, prefix: &str, local: &str) -> Result<Iri, ParseError> {
        match self.prefixes.get(prefix) {
            Some(base_iri) => Ok(Iri {
                value: format!("{}{}", base_iri, local),
            }),
            None => Err(self.error(format!("unknown prefix '{}:'", prefix))),
        }
    }

    // === Expression parsing ===
    //
    // Operator precedence (lowest to highest):
    //   1. OR  (||)
    //   2. AND (&&)
    //   3. Relational (=, !=, <, >, <=, >=, IN, NOT IN)
    //   4. Additive (+, -)
    //   5. Multiplicative (*, /)
    //   6. Unary (!, +, -)
    //   7. Primary (variable, literal, bracketed, built-in call)

    /// Parse a FILTER constraint (bracketed expression or built-in call).
    ///
    /// Grammar: `BrackettedExpression | BuiltInCall | FunctionCall`
    /// Special: `NOT EXISTS { ... }` and `EXISTS { ... }` are valid here.
    fn parse_constraint(&mut self) -> Result<Expression, ParseError> {
        match self.peek() {
            Some(Token::LParen) => {
                self.bump();
                let expr = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(expr)
            }
            Some(Token::Keyword(Keyword::Not)) => {
                // NOT EXISTS { pattern }
                self.bump();
                self.expect_keyword(Keyword::Exists)?;
                let pattern = self.parse_group_graph_pattern()?;
                Ok(Expression::NotExists(Box::new(pattern)))
            }
            Some(Token::Keyword(kw)) if is_builtin_call_keyword(*kw) => self.parse_builtin_call(),
            _ => Err(self.expected("filter constraint")),
        }
    }

    /// Parse a full expression.
    fn parse_expression(&mut self) -> Result<Expression, ParseError> {
        self.parse_or_expression()
    }

    fn parse_or_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_and_expression()?;
        while self.eat(&Token::PipePipe) {
            let right = self.parse_and_expression()?;
            left = Expression::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_relational_expression()?;
        while self.eat(&Token::AmpAmp) {
            let right = self.parse_relational_expression()?;
            left = Expression::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_relational_expression(&mut self) -> Result<Expression, ParseError> {
        let left = self.parse_additive_expression()?;
        match self.peek() {
            Some(Token::Eq) => {
                self.bump();
                let r = self.parse_additive_expression()?;
                Ok(Expression::Equal(Box::new(left), Box::new(r)))
            }
            Some(Token::NotEq) => {
                self.bump();
                let r = self.parse_additive_expression()?;
                Ok(Expression::NotEqual(Box::new(left), Box::new(r)))
            }
            Some(Token::Lt) => {
                self.bump();
                let r = self.parse_additive_expression()?;
                Ok(Expression::LessThan(Box::new(left), Box::new(r)))
            }
            Some(Token::Gt) => {
                self.bump();
                let r = self.parse_additive_expression()?;
                Ok(Expression::GreaterThan(Box::new(left), Box::new(r)))
            }
            Some(Token::LtEq) => {
                self.bump();
                let r = self.parse_additive_expression()?;
                Ok(Expression::LessThanOrEqual(Box::new(left), Box::new(r)))
            }
            Some(Token::GtEq) => {
                self.bump();
                let r = self.parse_additive_expression()?;
                Ok(Expression::GreaterThanOrEqual(Box::new(left), Box::new(r)))
            }
            Some(Token::Keyword(Keyword::In)) => {
                self.bump();
                let list = self.parse_expression_list()?;
                Ok(Expression::In(Box::new(left), list))
            }
            Some(Token::Keyword(Keyword::Not)) => {
                // Check for NOT IN
                let saved = self.pos;
                self.bump();
                if self.eat_keyword(Keyword::In) {
                    let list = self.parse_expression_list()?;
                    Ok(Expression::NotIn(Box::new(left), list))
                } else {
                    self.pos = saved;
                    Ok(left)
                }
            }
            _ => Ok(left),
        }
    }

    fn parse_additive_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_multiplicative_expression()?;
        loop {
            match self.peek() {
                Some(Token::Plus) => {
                    self.bump();
                    let right = self.parse_multiplicative_expression()?;
                    left = Expression::Add(Box::new(left), Box::new(right));
                }
                Some(Token::Minus) => {
                    self.bump();
                    let right = self.parse_multiplicative_expression()?;
                    left = Expression::Subtract(Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_multiplicative_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_unary_expression()?;
        loop {
            match self.peek() {
                Some(Token::Star) => {
                    self.bump();
                    let right = self.parse_unary_expression()?;
                    left = Expression::Multiply(Box::new(left), Box::new(right));
                }
                Some(Token::Slash) => {
                    self.bump();
                    let right = self.parse_unary_expression()?;
                    left = Expression::Divide(Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_unary_expression(&mut self) -> Result<Expression, ParseError> {
        match self.peek() {
            Some(Token::Bang) => {
                self.bump();
                let expr = self.parse_unary_expression()?;
                Ok(Expression::UnaryNot(Box::new(expr)))
            }
            Some(Token::Plus) => {
                self.bump();
                let expr = self.parse_unary_expression()?;
                Ok(Expression::UnaryPlus(Box::new(expr)))
            }
            Some(Token::Minus) => {
                self.bump();
                let expr = self.parse_unary_expression()?;
                Ok(Expression::UnaryMinus(Box::new(expr)))
            }
            _ => self.parse_primary_expression(),
        }
    }

    fn parse_primary_expression(&mut self) -> Result<Expression, ParseError> {
        match self.peek() {
            Some(Token::LParen) => {
                self.bump();
                let expr = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(expr)
            }
            Some(Token::Variable(_)) => {
                let var = self.parse_variable()?;
                Ok(Expression::Variable(var))
            }
            Some(Token::Iri(_)) | Some(Token::PrefixedName { .. }) => {
                let iri = self.parse_iri()?;
                // Check for function call: iri(args...)
                if matches!(self.peek(), Some(Token::LParen)) {
                    let args = self.parse_expression_list()?;
                    Ok(Expression::FunctionCall(iri, args))
                } else {
                    Ok(Expression::Iri(iri))
                }
            }
            Some(Token::StringLiteral(_)) | Some(Token::LongStringLiteral(_)) => {
                let lit = self.parse_rdf_literal()?;
                Ok(Expression::Literal(lit))
            }
            Some(Token::Integer(_)) | Some(Token::Decimal(_)) | Some(Token::Double(_)) => {
                if let Some(tok) = self.next_token() {
                    let s = match tok {
                        Token::Integer(s) | Token::Decimal(s) | Token::Double(s) => s,
                        _ => unreachable!(),
                    };
                    Ok(Expression::NumericLiteral(s))
                } else {
                    unreachable!()
                }
            }
            Some(Token::Keyword(Keyword::True)) => {
                self.bump();
                Ok(Expression::BooleanLiteral(true))
            }
            Some(Token::Keyword(Keyword::False)) => {
                self.bump();
                Ok(Expression::BooleanLiteral(false))
            }
            Some(Token::Keyword(kw)) if is_builtin_call_keyword(*kw) => self.parse_builtin_call(),
            Some(Token::Keyword(Keyword::Not)) => {
                // NOT EXISTS { pattern }
                self.bump(); // consume NOT
                self.expect_keyword(Keyword::Exists)?;
                let pattern = self.parse_group_graph_pattern()?;
                Ok(Expression::NotExists(Box::new(pattern)))
            }
            _ => Err(self.expected("expression")),
        }
    }

    /// Parse a parenthesized expression list: `(expr, expr, ...)`.
    fn parse_expression_list(&mut self) -> Result<Vec<Expression>, ParseError> {
        self.expect(&Token::LParen, "'('")?;
        let mut exprs = Vec::new();
        if !matches!(self.peek(), Some(Token::RParen)) {
            exprs.push(self.parse_expression()?);
            while self.eat(&Token::Comma) {
                exprs.push(self.parse_expression()?);
            }
        }
        self.expect(&Token::RParen, "')'")?;
        Ok(exprs)
    }

    /// Parse a built-in function call (BOUND, isIRI, STR, LANG, etc.).
    fn parse_builtin_call(&mut self) -> Result<Expression, ParseError> {
        let kw = match self.next_token() {
            Some(Token::Keyword(kw)) => kw,
            _ => return Err(self.expected("built-in function")),
        };

        match kw {
            // Type testing
            Keyword::Bound => {
                self.expect(&Token::LParen, "'('")?;
                let var = self.parse_variable()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::Bound(var))
            }
            Keyword::IsIri | Keyword::IsUri => {
                self.expect(&Token::LParen, "'('")?;
                let e = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::IsIri(Box::new(e)))
            }
            Keyword::IsBlank => {
                self.expect(&Token::LParen, "'('")?;
                let e = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::IsBlank(Box::new(e)))
            }
            Keyword::IsLiteral => {
                self.expect(&Token::LParen, "'('")?;
                let e = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::IsLiteral(Box::new(e)))
            }
            Keyword::IsNumeric => {
                self.expect(&Token::LParen, "'('")?;
                let e = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::IsNumeric(Box::new(e)))
            }

            // Accessors
            Keyword::Str => {
                self.expect(&Token::LParen, "'('")?;
                let e = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::Str(Box::new(e)))
            }
            Keyword::Lang => {
                self.expect(&Token::LParen, "'('")?;
                let e = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::Lang(Box::new(e)))
            }
            Keyword::Datatype => {
                self.expect(&Token::LParen, "'('")?;
                let e = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::Datatype(Box::new(e)))
            }

            // String functions
            Keyword::Strlen => {
                self.expect(&Token::LParen, "'('")?;
                let e = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::Strlen(Box::new(e)))
            }
            Keyword::Ucase => {
                self.expect(&Token::LParen, "'('")?;
                let e = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::Ucase(Box::new(e)))
            }
            Keyword::Lcase => {
                self.expect(&Token::LParen, "'('")?;
                let e = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::Lcase(Box::new(e)))
            }
            Keyword::Contains => {
                self.expect(&Token::LParen, "'('")?;
                let a = self.parse_expression()?;
                self.expect(&Token::Comma, "','")?;
                let b = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::Contains(Box::new(a), Box::new(b)))
            }
            Keyword::Strstarts => {
                self.expect(&Token::LParen, "'('")?;
                let a = self.parse_expression()?;
                self.expect(&Token::Comma, "','")?;
                let b = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::StrStarts(Box::new(a), Box::new(b)))
            }
            Keyword::Strends => {
                self.expect(&Token::LParen, "'('")?;
                let a = self.parse_expression()?;
                self.expect(&Token::Comma, "','")?;
                let b = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::StrEnds(Box::new(a), Box::new(b)))
            }
            Keyword::Substr => {
                self.expect(&Token::LParen, "'('")?;
                let source = self.parse_expression()?;
                self.expect(&Token::Comma, "','")?;
                let start = self.parse_expression()?;
                let length = if self.eat(&Token::Comma) {
                    Some(Box::new(self.parse_expression()?))
                } else {
                    None
                };
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::Substr(
                    Box::new(source),
                    Box::new(start),
                    length,
                ))
            }
            Keyword::Concat => {
                let args = self.parse_expression_list()?;
                Ok(Expression::Concat(args))
            }
            Keyword::Replace => {
                self.expect(&Token::LParen, "'('")?;
                let arg = self.parse_expression()?;
                self.expect(&Token::Comma, "','")?;
                let pattern = self.parse_expression()?;
                self.expect(&Token::Comma, "','")?;
                let replacement = self.parse_expression()?;
                let flags = if self.eat(&Token::Comma) {
                    Some(Box::new(self.parse_expression()?))
                } else {
                    None
                };
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::Replace(
                    Box::new(arg),
                    Box::new(pattern),
                    Box::new(replacement),
                    flags,
                ))
            }
            Keyword::Regex => {
                self.expect(&Token::LParen, "'('")?;
                let text = self.parse_expression()?;
                self.expect(&Token::Comma, "','")?;
                let pattern = self.parse_expression()?;
                let flags = if self.eat(&Token::Comma) {
                    Some(Box::new(self.parse_expression()?))
                } else {
                    None
                };
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::Regex(Box::new(text), Box::new(pattern), flags))
            }

            // Conditional
            Keyword::If => {
                self.expect(&Token::LParen, "'('")?;
                let cond = self.parse_expression()?;
                self.expect(&Token::Comma, "','")?;
                let then_expr = self.parse_expression()?;
                self.expect(&Token::Comma, "','")?;
                let else_expr = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                Ok(Expression::If(
                    Box::new(cond),
                    Box::new(then_expr),
                    Box::new(else_expr),
                ))
            }
            Keyword::Coalesce => {
                let args = self.parse_expression_list()?;
                Ok(Expression::Coalesce(args))
            }

            // Two-argument functions → mapped to FunctionCall
            Keyword::Langmatches | Keyword::SameTerm | Keyword::StrDt | Keyword::StrLang => {
                self.expect(&Token::LParen, "'('")?;
                let a = self.parse_expression()?;
                self.expect(&Token::Comma, "','")?;
                let b = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                let name = format!("{}", kw);
                Ok(Expression::FunctionCall(Iri { value: name }, vec![a, b]))
            }

            // One-argument functions → mapped to FunctionCall
            Keyword::Abs
            | Keyword::Round
            | Keyword::Ceil
            | Keyword::Floor
            | Keyword::Md5
            | Keyword::Sha1
            | Keyword::Sha256
            | Keyword::Sha384
            | Keyword::Sha512
            | Keyword::EncodeForUri
            | Keyword::Year
            | Keyword::Month
            | Keyword::Day
            | Keyword::Hours
            | Keyword::Minutes
            | Keyword::Seconds
            | Keyword::Timezone
            | Keyword::Tz
            | Keyword::Iri
            | Keyword::Uri
            | Keyword::Bnode => {
                self.expect(&Token::LParen, "'('")?;
                let e = self.parse_expression()?;
                self.expect(&Token::RParen, "')'")?;
                let name = format!("{}", kw);
                Ok(Expression::FunctionCall(Iri { value: name }, vec![e]))
            }

            // Zero-argument functions
            Keyword::Now | Keyword::Rand | Keyword::Uuid | Keyword::Struuid => {
                self.expect(&Token::LParen, "'('")?;
                self.expect(&Token::RParen, "')'")?;
                let name = format!("{}", kw);
                Ok(Expression::FunctionCall(Iri { value: name }, vec![]))
            }

            Keyword::Exists => {
                let pattern = self.parse_group_graph_pattern()?;
                Ok(Expression::Exists(Box::new(pattern)))
            }

            _ => Err(self.error(format!("unexpected keyword {:?} in expression", kw))),
        }
    }
}

/// Check whether a keyword is a built-in function that can appear in expression
/// position.
fn is_builtin_call_keyword(kw: Keyword) -> bool {
    matches!(
        kw,
        Keyword::Bound
            | Keyword::IsIri
            | Keyword::IsUri
            | Keyword::IsBlank
            | Keyword::IsLiteral
            | Keyword::IsNumeric
            | Keyword::Str
            | Keyword::Lang
            | Keyword::Datatype
            | Keyword::Strlen
            | Keyword::Ucase
            | Keyword::Lcase
            | Keyword::Contains
            | Keyword::Strstarts
            | Keyword::Strends
            | Keyword::Substr
            | Keyword::Concat
            | Keyword::Replace
            | Keyword::Regex
            | Keyword::If
            | Keyword::Coalesce
            | Keyword::Langmatches
            | Keyword::SameTerm
            | Keyword::StrDt
            | Keyword::StrLang
            | Keyword::EncodeForUri
            | Keyword::Uuid
            | Keyword::Struuid
            | Keyword::Abs
            | Keyword::Round
            | Keyword::Ceil
            | Keyword::Floor
            | Keyword::Rand
            | Keyword::Now
            | Keyword::Year
            | Keyword::Month
            | Keyword::Day
            | Keyword::Hours
            | Keyword::Minutes
            | Keyword::Seconds
            | Keyword::Timezone
            | Keyword::Tz
            | Keyword::Md5
            | Keyword::Sha1
            | Keyword::Sha256
            | Keyword::Sha384
            | Keyword::Sha512
            | Keyword::Bnode
            | Keyword::Iri
            | Keyword::Uri
            | Keyword::Exists
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    // Helper to parse and unwrap.
    fn p(input: &str) -> SparqlQuery {
        parse(input).unwrap_or_else(|e| panic!("parse error: {}", e))
    }

    // === Basic structure tests ===

    #[test]
    fn test_simple_select() {
        let q = p("SELECT ?s ?p ?o WHERE { ?s ?p ?o }");
        assert!(matches!(
            q.form,
            QueryForm::Select {
                modifier: SelectModifier::Default,
                projection: SelectClause::Variables(_),
            }
        ));
        if let QueryForm::Select {
            projection: SelectClause::Variables(vars),
            ..
        } = &q.form
        {
            assert_eq!(vars.len(), 3);
            assert!(matches!(&vars[0], SelectVariable::Variable(v) if v.name == "s"));
            assert!(matches!(&vars[1], SelectVariable::Variable(v) if v.name == "p"));
            assert!(matches!(&vars[2], SelectVariable::Variable(v) if v.name == "o"));
        }
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert_eq!(triples.len(), 1);
            assert!(matches!(&triples[0].subject, VarOrTerm::Variable(v) if v.name == "s"));
            assert!(matches!(&triples[0].predicate, VerbPath::Variable(v) if v.name == "p"));
            assert!(matches!(&triples[0].object, VarOrTerm::Variable(v) if v.name == "o"));
        } else {
            panic!("expected Basic pattern, got {:?}", q.where_clause);
        }
    }

    #[test]
    fn test_select_star() {
        let q = p("SELECT * WHERE { ?s ?p ?o }");
        assert!(matches!(
            q.form,
            QueryForm::Select {
                projection: SelectClause::Wildcard,
                ..
            }
        ));
    }

    #[test]
    fn test_select_distinct() {
        let q = p("SELECT DISTINCT ?s WHERE { ?s ?p ?o }");
        assert!(matches!(
            q.form,
            QueryForm::Select {
                modifier: SelectModifier::Distinct,
                ..
            }
        ));
    }

    #[test]
    fn test_prefix_resolution() {
        let q = p(r#"PREFIX ex: <http://example.org/>
SELECT ?s WHERE { ?s ex:name "Alice" }"#);
        assert_eq!(q.prefixes.len(), 1);
        assert_eq!(q.prefixes[0].prefix, "ex");
        assert_eq!(q.prefixes[0].iri.value, "http://example.org/");

        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert_eq!(triples.len(), 1);
            // Predicate should be resolved
            if let VerbPath::Path(PropertyPath::Iri(iri)) = &triples[0].predicate {
                assert_eq!(iri.value, "http://example.org/name");
            } else {
                panic!("expected IRI predicate");
            }
            // Object should be a simple string literal
            assert!(matches!(
                &triples[0].object,
                VarOrTerm::Term(GraphTerm::Literal(RdfLiteral::Simple(s))) if s == "Alice"
            ));
        } else {
            panic!("expected Basic pattern");
        }
    }

    #[test]
    fn test_multiple_prefixes() {
        let q = p(r#"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?name WHERE { ?x rdf:type foaf:Person . ?x foaf:name ?name }"#);
        assert_eq!(q.prefixes.len(), 2);
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert_eq!(triples.len(), 2);
            // First triple: ?x rdf:type foaf:Person
            if let VerbPath::Path(PropertyPath::Iri(iri)) = &triples[0].predicate {
                assert_eq!(iri.value, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
            }
            if let VarOrTerm::Term(GraphTerm::Iri(iri)) = &triples[0].object {
                assert_eq!(iri.value, "http://xmlns.com/foaf/0.1/Person");
            }
        } else {
            panic!("expected Basic pattern");
        }
    }

    // === Multiple triple patterns ===

    #[test]
    fn test_multiple_triples_with_dot() {
        let q = p("SELECT ?s ?o WHERE { ?s ?p ?o . ?s ?p2 ?o2 }");
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert_eq!(triples.len(), 2);
        } else {
            panic!("expected Basic pattern");
        }
    }

    #[test]
    fn test_semicolon_shorthand() {
        // ?s ?p1 ?o1 ; ?p2 ?o2 → two triples sharing subject ?s
        let q = p("SELECT * WHERE { ?s <http://ex.org/p1> ?o1 ; <http://ex.org/p2> ?o2 }");
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert_eq!(triples.len(), 2);
            // Both triples should share the same subject
            assert_eq!(triples[0].subject, triples[1].subject);
            // But different predicates
            assert_ne!(triples[0].predicate, triples[1].predicate);
        } else {
            panic!("expected Basic pattern");
        }
    }

    #[test]
    fn test_comma_shorthand() {
        // ?s ?p ?o1, ?o2 → two triples sharing subject and predicate
        let q = p("SELECT * WHERE { ?s ?p ?o1, ?o2 }");
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert_eq!(triples.len(), 2);
            assert_eq!(triples[0].subject, triples[1].subject);
            assert_eq!(triples[0].predicate, triples[1].predicate);
            assert_ne!(triples[0].object, triples[1].object);
        } else {
            panic!("expected Basic pattern");
        }
    }

    #[test]
    fn test_rdf_type_shorthand() {
        let q = p("SELECT ?s ?type WHERE { ?s a ?type }");
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            if let VerbPath::Path(PropertyPath::Iri(iri)) = &triples[0].predicate {
                assert_eq!(iri.value, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
            } else {
                panic!("expected IRI predicate for 'a'");
            }
        } else {
            panic!("expected Basic pattern");
        }
    }

    // === FILTER tests ===

    #[test]
    fn test_filter_simple_comparison() {
        let q = p("SELECT ?s WHERE { ?s ?p ?o . FILTER(?o > 42) }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            assert_eq!(patterns.len(), 2);
            assert!(matches!(&patterns[0], GroupGraphPattern::Basic(_)));
            if let GroupGraphPattern::Filter(expr) = &patterns[1] {
                assert!(matches!(expr, Expression::GreaterThan(_, _)));
            } else {
                panic!("expected Filter pattern");
            }
        } else {
            panic!("expected Group pattern, got {:?}", q.where_clause);
        }
    }

    #[test]
    fn test_filter_compound_and_or() {
        let q = p("SELECT ?s WHERE { ?s ?p ?o . FILTER(?o > 42 && ?o < 100) }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Filter(expr) = &patterns[1] {
                if let Expression::And(left, right) = expr {
                    assert!(matches!(left.as_ref(), Expression::GreaterThan(_, _)));
                    assert!(matches!(right.as_ref(), Expression::LessThan(_, _)));
                } else {
                    panic!("expected AND expression, got {:?}", expr);
                }
            }
        } else {
            panic!("expected Group pattern");
        }
    }

    #[test]
    fn test_filter_or() {
        let q = p("SELECT ?s WHERE { ?s ?p ?o . FILTER(?o = 1 || ?o = 2) }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Filter(expr) = &patterns[1] {
                assert!(matches!(expr, Expression::Or(_, _)));
            }
        }
    }

    #[test]
    fn test_filter_bound() {
        let q = p("SELECT ?s WHERE { ?s ?p ?o . FILTER(BOUND(?o)) }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Filter(expr) = &patterns[1] {
                if let Expression::Bound(var) = expr {
                    assert_eq!(var.name, "o");
                } else {
                    panic!("expected BOUND expression");
                }
            }
        }
    }

    #[test]
    fn test_filter_isiri() {
        let q = p("SELECT ?s WHERE { ?s ?p ?o . FILTER(isIRI(?s)) }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Filter(expr) = &patterns[1] {
                assert!(matches!(expr, Expression::IsIri(_)));
            }
        }
    }

    #[test]
    fn test_filter_str_equality() {
        let q = p(r#"SELECT ?s WHERE { ?s ?p ?o . FILTER(STR(?o) = "hello") }"#);
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Filter(expr) = &patterns[1] {
                if let Expression::Equal(left, right) = expr {
                    assert!(matches!(left.as_ref(), Expression::Str(_)));
                    assert!(matches!(
                        right.as_ref(),
                        Expression::Literal(RdfLiteral::Simple(s)) if s == "hello"
                    ));
                } else {
                    panic!("expected Equal expression");
                }
            }
        }
    }

    #[test]
    fn test_filter_lang() {
        let q = p(r#"SELECT ?s WHERE { ?s ?p ?o . FILTER(LANG(?o) = "en") }"#);
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Filter(expr) = &patterns[1] {
                if let Expression::Equal(left, _) = expr {
                    assert!(matches!(left.as_ref(), Expression::Lang(_)));
                }
            }
        }
    }

    #[test]
    fn test_filter_not() {
        let q = p("SELECT ?s WHERE { ?s ?p ?o . FILTER(!(?o = 42)) }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Filter(expr) = &patterns[1] {
                if let Expression::UnaryNot(inner) = expr {
                    assert!(matches!(inner.as_ref(), Expression::Equal(_, _)));
                } else {
                    panic!("expected UnaryNot");
                }
            }
        }
    }

    #[test]
    fn test_filter_datatype() {
        let q = p(
            "SELECT ?s WHERE { ?s ?p ?o . FILTER(DATATYPE(?o) = <http://www.w3.org/2001/XMLSchema#integer>) }",
        );
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Filter(expr) = &patterns[1] {
                if let Expression::Equal(left, right) = expr {
                    assert!(matches!(left.as_ref(), Expression::Datatype(_)));
                    assert!(matches!(right.as_ref(), Expression::Iri(_)));
                }
            }
        }
    }

    #[test]
    fn test_filter_arithmetic() {
        let q = p("SELECT ?s WHERE { ?s ?p ?o . FILTER(?o + 1 > 10 * 2) }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Filter(expr) = &patterns[1] {
                // Should be: GreaterThan(Add(?o, 1), Multiply(10, 2))
                if let Expression::GreaterThan(left, right) = expr {
                    assert!(matches!(left.as_ref(), Expression::Add(_, _)));
                    assert!(matches!(right.as_ref(), Expression::Multiply(_, _)));
                } else {
                    panic!("expected GreaterThan, got {:?}", expr);
                }
            }
        }
    }

    #[test]
    fn test_filter_in() {
        let q = p(r#"SELECT ?s WHERE { ?s ?p ?o . FILTER(?o IN (1, 2, 3)) }"#);
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Filter(expr) = &patterns[1] {
                if let Expression::In(_, list) = expr {
                    assert_eq!(list.len(), 3);
                } else {
                    panic!("expected In expression");
                }
            }
        }
    }

    #[test]
    fn test_filter_regex() {
        let q = p(r#"SELECT ?s WHERE { ?s ?p ?o . FILTER(REGEX(?o, "^hello", "i")) }"#);
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Filter(expr) = &patterns[1] {
                if let Expression::Regex(_, _, flags) = expr {
                    assert!(flags.is_some());
                } else {
                    panic!("expected Regex expression");
                }
            }
        }
    }

    // === Typed and language-tagged literals in patterns ===

    #[test]
    fn test_typed_literal_in_pattern() {
        let q = p(r#"SELECT ?s WHERE { ?s ?p "42"^^<http://www.w3.org/2001/XMLSchema#integer> }"#);
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            if let VarOrTerm::Term(GraphTerm::Literal(RdfLiteral::Typed { value, datatype })) =
                &triples[0].object
            {
                assert_eq!(value, "42");
                assert_eq!(datatype.value, "http://www.w3.org/2001/XMLSchema#integer");
            } else {
                panic!("expected typed literal");
            }
        }
    }

    #[test]
    fn test_language_tagged_literal_in_pattern() {
        let q = p(r#"SELECT ?s WHERE { ?s ?p "chat"@fr }"#);
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            if let VarOrTerm::Term(GraphTerm::Literal(RdfLiteral::LanguageTagged {
                value,
                language,
            })) = &triples[0].object
            {
                assert_eq!(value, "chat");
                assert_eq!(language, "fr");
            } else {
                panic!("expected language-tagged literal");
            }
        }
    }

    // === WHERE keyword optional ===

    #[test]
    fn test_where_keyword_optional() {
        let q = p("SELECT ?s ?p ?o { ?s ?p ?o }");
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert_eq!(triples.len(), 1);
        }
    }

    // === Select expression ===

    #[test]
    fn test_select_expression() {
        let q = p("SELECT ?s (?o + 1 AS ?inc) WHERE { ?s ?p ?o }");
        if let QueryForm::Select {
            projection: SelectClause::Variables(vars),
            ..
        } = &q.form
        {
            assert_eq!(vars.len(), 2);
            assert!(matches!(&vars[0], SelectVariable::Variable(v) if v.name == "s"));
            if let SelectVariable::Expression(expr, var) = &vars[1] {
                assert_eq!(var.name, "inc");
                assert!(matches!(expr, Expression::Add(_, _)));
            } else {
                panic!("expected expression in SELECT");
            }
        }
    }

    // === Error tests ===

    #[test]
    fn test_error_missing_brace() {
        let err = parse("SELECT ?s WHERE ?s ?p ?o }").unwrap_err();
        assert!(err.message.contains("'{'"), "got: {}", err.message);
    }

    #[test]
    fn test_error_unknown_prefix() {
        let err = parse("SELECT ?s WHERE { ?s ex:name ?o }").unwrap_err();
        assert!(
            err.message.contains("unknown prefix"),
            "got: {}",
            err.message
        );
    }

    #[test]
    fn test_error_missing_select() {
        let err = parse("WHERE { ?s ?p ?o }").unwrap_err();
        assert!(err.message.contains("SELECT"), "got: {}", err.message);
    }

    #[test]
    fn test_error_empty_select_variables() {
        let err = parse("SELECT WHERE { ?s ?p ?o }").unwrap_err();
        assert!(
            err.message.contains("variable or '*'"),
            "got: {}",
            err.message
        );
    }

    #[test]
    fn test_error_lexer_propagates() {
        let err = parse("SELECT ?s WHERE { ?s <http://broken ").unwrap_err();
        assert!(
            err.message.contains("unterminated IRI"),
            "got: {}",
            err.message
        );
    }

    // === Full IRI in patterns ===

    #[test]
    fn test_full_iri_in_patterns() {
        let q = p(
            "SELECT ?s WHERE { ?s <http://example.org/name> ?o . ?o <http://example.org/age> ?a }",
        );
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert_eq!(triples.len(), 2);
            if let VerbPath::Path(PropertyPath::Iri(iri)) = &triples[0].predicate {
                assert_eq!(iri.value, "http://example.org/name");
            }
            if let VerbPath::Path(PropertyPath::Iri(iri)) = &triples[1].predicate {
                assert_eq!(iri.value, "http://example.org/age");
            }
        }
    }

    // === Empty WHERE clause ===

    #[test]
    fn test_empty_where() {
        let q = p("SELECT * WHERE { }");
        assert!(matches!(q.where_clause, GroupGraphPattern::Basic(ref t) if t.is_empty()));
    }

    // === Numeric literal in pattern ===

    #[test]
    fn test_numeric_object() {
        let q = p("SELECT ?s WHERE { ?s ?p 42 }");
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert!(matches!(
                &triples[0].object,
                VarOrTerm::Term(GraphTerm::NumericLiteral(s)) if s == "42"
            ));
        }
    }

    // === Boolean literal ===

    #[test]
    fn test_boolean_object() {
        let q = p("SELECT ?s WHERE { ?s ?p true }");
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert!(matches!(
                &triples[0].object,
                VarOrTerm::Term(GraphTerm::BooleanLiteral(true))
            ));
        }
    }

    // === Blank node in pattern ===

    #[test]
    fn test_blank_node_subject() {
        let q = p("SELECT ?p ?o WHERE { _:b0 ?p ?o }");
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert!(matches!(
                &triples[0].subject,
                VarOrTerm::Term(GraphTerm::BlankNode(label)) if label == "b0"
            ));
        }
    }

    // === Complex query combining many features ===

    #[test]
    fn test_complex_query() {
        let q = p(r#"PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT DISTINCT ?name ?email
WHERE {
  ?person a foaf:Person .
  ?person foaf:name ?name .
  ?person foaf:mbox ?email .
  FILTER(CONTAINS(?name, "Alice") && BOUND(?email))
}"#);
        assert_eq!(q.prefixes.len(), 2);
        assert!(matches!(
            q.form,
            QueryForm::Select {
                modifier: SelectModifier::Distinct,
                ..
            }
        ));
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            // Should have Basic + Filter
            assert_eq!(patterns.len(), 2);
            if let GroupGraphPattern::Basic(triples) = &patterns[0] {
                assert_eq!(triples.len(), 3);
            }
            if let GroupGraphPattern::Filter(expr) = &patterns[1] {
                assert!(matches!(expr, Expression::And(_, _)));
            }
        }
    }

    // === OPTIONAL tests ===

    #[test]
    fn test_optional_simple() {
        let q = p("SELECT ?s ?o WHERE { ?s ?p ?o . OPTIONAL { ?s <http://ex.org/q> ?q } }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            assert_eq!(patterns.len(), 2);
            assert!(matches!(&patterns[0], GroupGraphPattern::Basic(_)));
            if let GroupGraphPattern::Optional(inner) = &patterns[1] {
                if let GroupGraphPattern::Basic(triples) = inner.as_ref() {
                    assert_eq!(triples.len(), 1);
                } else {
                    panic!("expected Basic inside OPTIONAL, got {:?}", inner);
                }
            } else {
                panic!("expected Optional pattern, got {:?}", patterns[1]);
            }
        } else {
            panic!("expected Group pattern, got {:?}", q.where_clause);
        }
    }

    #[test]
    fn test_optional_with_filter() {
        // OPTIONAL with FILTER inside — critical for correct semantics
        let q = p(
            "SELECT ?s ?z WHERE { ?s ?p ?o . OPTIONAL { ?o <http://ex.org/q> ?z . FILTER(?z > 5) } }",
        );
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            assert_eq!(patterns.len(), 2);
            if let GroupGraphPattern::Optional(inner) = &patterns[1] {
                if let GroupGraphPattern::Group(inner_patterns) = inner.as_ref() {
                    assert_eq!(inner_patterns.len(), 2);
                    assert!(matches!(&inner_patterns[0], GroupGraphPattern::Basic(_)));
                    assert!(matches!(&inner_patterns[1], GroupGraphPattern::Filter(_)));
                } else {
                    panic!("expected Group inside OPTIONAL");
                }
            }
        }
    }

    #[test]
    fn test_multiple_optionals() {
        let q = p(
            "SELECT * WHERE { ?s ?p ?o . OPTIONAL { ?s <http://ex.org/a> ?a } OPTIONAL { ?s <http://ex.org/b> ?b } }",
        );
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            assert_eq!(patterns.len(), 3);
            assert!(matches!(&patterns[0], GroupGraphPattern::Basic(_)));
            assert!(matches!(&patterns[1], GroupGraphPattern::Optional(_)));
            assert!(matches!(&patterns[2], GroupGraphPattern::Optional(_)));
        }
    }

    // === UNION tests ===

    #[test]
    fn test_union_simple() {
        let q =
            p("SELECT ?s WHERE { { ?s <http://ex.org/a> ?o } UNION { ?s <http://ex.org/b> ?o } }");
        if let GroupGraphPattern::Union(left, right) = &q.where_clause {
            assert!(matches!(left.as_ref(), GroupGraphPattern::Basic(_)));
            assert!(matches!(right.as_ref(), GroupGraphPattern::Basic(_)));
        } else {
            panic!("expected Union pattern, got {:?}", q.where_clause);
        }
    }

    #[test]
    fn test_union_three_way() {
        let q = p("SELECT ?s WHERE { { ?s ?p 1 } UNION { ?s ?p 2 } UNION { ?s ?p 3 } }");
        // Should be Union(Union(1, 2), 3) — left-associative
        if let GroupGraphPattern::Union(left, right) = &q.where_clause {
            assert!(matches!(left.as_ref(), GroupGraphPattern::Union(_, _)));
            assert!(matches!(right.as_ref(), GroupGraphPattern::Basic(_)));
        } else {
            panic!("expected nested Union pattern");
        }
    }

    #[test]
    fn test_union_with_triples() {
        // Triples before UNION
        let q = p(
            "SELECT * WHERE { ?s ?p ?o . { ?s <http://ex.org/a> ?a } UNION { ?s <http://ex.org/b> ?b } }",
        );
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            assert_eq!(patterns.len(), 2);
            assert!(matches!(&patterns[0], GroupGraphPattern::Basic(_)));
            assert!(matches!(&patterns[1], GroupGraphPattern::Union(_, _)));
        }
    }

    // === MINUS tests ===

    #[test]
    fn test_minus_simple() {
        let q =
            p("SELECT ?s WHERE { ?s <http://ex.org/p> ?o . MINUS { ?s <http://ex.org/q> ?z } }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            assert_eq!(patterns.len(), 2);
            assert!(matches!(&patterns[0], GroupGraphPattern::Basic(_)));
            if let GroupGraphPattern::Minus(inner) = &patterns[1] {
                if let GroupGraphPattern::Basic(triples) = inner.as_ref() {
                    assert_eq!(triples.len(), 1);
                }
            } else {
                panic!("expected Minus pattern");
            }
        }
    }

    #[test]
    fn test_minus_disjoint_variables() {
        // MINUS with no shared variables — should still parse
        let q = p("SELECT ?s WHERE { ?s ?p ?o . MINUS { ?a ?b ?c } }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            assert_eq!(patterns.len(), 2);
            assert!(matches!(&patterns[1], GroupGraphPattern::Minus(_)));
        }
    }

    // === BIND tests ===

    #[test]
    fn test_bind_simple() {
        let q = p("SELECT ?s ?label WHERE { ?s ?p ?o . BIND(STR(?o) AS ?label) }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            assert_eq!(patterns.len(), 2);
            if let GroupGraphPattern::Bind(expr, var) = &patterns[1] {
                assert_eq!(var.name, "label");
                assert!(matches!(expr, Expression::Str(_)));
            } else {
                panic!("expected Bind pattern, got {:?}", patterns[1]);
            }
        }
    }

    #[test]
    fn test_bind_arithmetic() {
        let q =
            p("SELECT ?x ?doubled WHERE { ?x <http://ex.org/val> ?v . BIND(?v * 2 AS ?doubled) }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Bind(expr, var) = &patterns[1] {
                assert_eq!(var.name, "doubled");
                assert!(matches!(expr, Expression::Multiply(_, _)));
            }
        }
    }

    // === VALUES tests ===

    #[test]
    fn test_values_single_var() {
        let q = p("SELECT ?s WHERE { ?s ?p ?o . VALUES ?o { 1 2 3 } }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            assert_eq!(patterns.len(), 2);
            if let GroupGraphPattern::Values { variables, rows } = &patterns[1] {
                assert_eq!(variables.len(), 1);
                assert_eq!(variables[0].name, "o");
                assert_eq!(rows.len(), 3);
                // Each row has one value
                assert!(matches!(&rows[0][0], Some(GraphTerm::NumericLiteral(s)) if s == "1"));
                assert!(matches!(&rows[1][0], Some(GraphTerm::NumericLiteral(s)) if s == "2"));
                assert!(matches!(&rows[2][0], Some(GraphTerm::NumericLiteral(s)) if s == "3"));
            } else {
                panic!("expected Values pattern, got {:?}", patterns[1]);
            }
        }
    }

    #[test]
    fn test_values_multi_var() {
        let q = p(r#"SELECT ?s ?type WHERE {
            ?s a ?type .
            VALUES (?s ?type) { (<http://ex.org/a> <http://ex.org/T1>) (<http://ex.org/b> <http://ex.org/T2>) }
        }"#);
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Values { variables, rows } = &patterns[1] {
                assert_eq!(variables.len(), 2);
                assert_eq!(variables[0].name, "s");
                assert_eq!(variables[1].name, "type");
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].len(), 2);
            }
        }
    }

    #[test]
    fn test_values_with_undef() {
        let q = p(r#"SELECT * WHERE {
            ?s ?p ?o .
            VALUES (?x ?y) { (1 2) (3 UNDEF) }
        }"#);
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Values { rows, .. } = &patterns[1] {
                assert_eq!(rows.len(), 2);
                // First row: both defined
                assert!(rows[0][0].is_some());
                assert!(rows[0][1].is_some());
                // Second row: second value is UNDEF
                assert!(rows[1][0].is_some());
                assert!(rows[1][1].is_none());
            }
        }
    }

    #[test]
    fn test_values_string_literals() {
        let q = p(r#"SELECT ?s WHERE { ?s ?p ?name . VALUES ?name { "Alice" "Bob" } }"#);
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Values { variables, rows } = &patterns[1] {
                assert_eq!(variables[0].name, "name");
                assert_eq!(rows.len(), 2);
                assert!(matches!(
                    &rows[0][0],
                    Some(GraphTerm::Literal(RdfLiteral::Simple(s))) if s == "Alice"
                ));
            }
        }
    }

    // === EXISTS / NOT EXISTS tests ===

    #[test]
    fn test_filter_exists() {
        let q = p("SELECT ?s WHERE { ?s ?p ?o . FILTER EXISTS { ?s <http://ex.org/q> ?z } }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Filter(expr) = &patterns[1] {
                if let Expression::Exists(pattern) = expr {
                    assert!(matches!(pattern.as_ref(), GroupGraphPattern::Basic(_)));
                } else {
                    panic!("expected Exists expression, got {:?}", expr);
                }
            }
        }
    }

    #[test]
    fn test_filter_not_exists() {
        let q = p("SELECT ?s WHERE { ?s ?p ?o . FILTER NOT EXISTS { ?s <http://ex.org/q> ?z } }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Filter(expr) = &patterns[1] {
                if let Expression::NotExists(pattern) = expr {
                    assert!(matches!(pattern.as_ref(), GroupGraphPattern::Basic(_)));
                } else {
                    panic!("expected NotExists expression, got {:?}", expr);
                }
            }
        }
    }

    #[test]
    fn test_not_exists_in_expression() {
        // NOT EXISTS used inside a compound expression
        let q = p("SELECT ?s WHERE { ?s ?p ?o . FILTER(!NOT EXISTS { ?s <http://ex.org/q> ?z }) }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            if let GroupGraphPattern::Filter(expr) = &patterns[1] {
                if let Expression::UnaryNot(inner) = expr {
                    assert!(matches!(inner.as_ref(), Expression::NotExists(_)));
                } else {
                    panic!("expected UnaryNot(NotExists), got {:?}", expr);
                }
            }
        }
    }

    // === Nested groups ===

    #[test]
    fn test_nested_group() {
        let q = p("SELECT ?s WHERE { ?s ?p ?o . { ?o <http://ex.org/r> ?r } }");
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            assert_eq!(patterns.len(), 2);
            assert!(matches!(&patterns[0], GroupGraphPattern::Basic(_)));
            assert!(matches!(&patterns[1], GroupGraphPattern::Basic(_)));
        }
    }

    // === Combination tests ===

    #[test]
    fn test_optional_inside_union() {
        let q = p(
            "SELECT * WHERE { { ?s <http://ex.org/a> ?o . OPTIONAL { ?o <http://ex.org/b> ?z } } UNION { ?s <http://ex.org/c> ?o } }",
        );
        if let GroupGraphPattern::Union(left, right) = &q.where_clause {
            if let GroupGraphPattern::Group(left_patterns) = left.as_ref() {
                assert_eq!(left_patterns.len(), 2);
                assert!(matches!(&left_patterns[0], GroupGraphPattern::Basic(_)));
                assert!(matches!(&left_patterns[1], GroupGraphPattern::Optional(_)));
            } else {
                panic!("expected Group in left branch of UNION");
            }
            assert!(matches!(right.as_ref(), GroupGraphPattern::Basic(_)));
        }
    }

    #[test]
    fn test_optional_and_minus_combined() {
        let q = p(
            "SELECT ?s WHERE { ?s ?p ?o . OPTIONAL { ?s <http://ex.org/a> ?a } MINUS { ?s <http://ex.org/b> ?b } }",
        );
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            assert_eq!(patterns.len(), 3);
            assert!(matches!(&patterns[0], GroupGraphPattern::Basic(_)));
            assert!(matches!(&patterns[1], GroupGraphPattern::Optional(_)));
            assert!(matches!(&patterns[2], GroupGraphPattern::Minus(_)));
        }
    }

    #[test]
    fn test_bind_values_combined() {
        let q = p(r#"SELECT ?s ?label WHERE {
            VALUES ?type { <http://ex.org/T1> <http://ex.org/T2> }
            ?s a ?type .
            ?s <http://ex.org/name> ?name .
            BIND(UCASE(?name) AS ?label)
        }"#);
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            assert_eq!(patterns.len(), 3);
            assert!(matches!(&patterns[0], GroupGraphPattern::Values { .. }));
            assert!(matches!(&patterns[1], GroupGraphPattern::Basic(_)));
            assert!(matches!(&patterns[2], GroupGraphPattern::Bind(_, _)));
        }
    }

    #[test]
    fn test_complex_with_all_forms() {
        // Test combining OPTIONAL, UNION, MINUS, BIND, VALUES, FILTER, EXISTS
        let q = p(r#"PREFIX ex: <http://example.org/>
SELECT ?s ?label ?extra WHERE {
  VALUES ?type { ex:Person ex:Org }
  ?s a ?type .
  ?s ex:name ?name .
  OPTIONAL { ?s ex:email ?email }
  BIND(UCASE(?name) AS ?label)
  FILTER(BOUND(?email))
  MINUS { ?s ex:deleted true }
  { ?s ex:extra ?extra } UNION { ?s ex:bonus ?extra }
}"#);
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            assert_eq!(patterns.len(), 7);
            assert!(matches!(&patterns[0], GroupGraphPattern::Values { .. }));
            assert!(matches!(&patterns[1], GroupGraphPattern::Basic(_)));
            assert!(matches!(&patterns[2], GroupGraphPattern::Optional(_)));
            assert!(matches!(&patterns[3], GroupGraphPattern::Bind(_, _)));
            assert!(matches!(&patterns[4], GroupGraphPattern::Filter(_)));
            assert!(matches!(&patterns[5], GroupGraphPattern::Minus(_)));
            assert!(matches!(&patterns[6], GroupGraphPattern::Union(_, _)));
        } else {
            panic!("expected Group pattern, got {:?}", q.where_clause);
        }
    }

    // === CONSTRUCT tests ===

    #[test]
    fn test_construct_basic() {
        let q = p(r#"CONSTRUCT { ?s <http://example.org/knows> ?o }
WHERE { ?s <http://example.org/friend> ?o }"#);
        if let QueryForm::Construct { template } = &q.form {
            assert_eq!(template.len(), 1);
            assert!(matches!(&template[0].subject, VarOrTerm::Variable(v) if v.name == "s"));
            assert!(matches!(
                &template[0].predicate,
                VerbPath::Path(PropertyPath::Iri(iri)) if iri.value == "http://example.org/knows"
            ));
            assert!(matches!(&template[0].object, VarOrTerm::Variable(v) if v.name == "o"));
        } else {
            panic!("expected Construct, got {:?}", q.form);
        }
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert_eq!(triples.len(), 1);
        } else {
            panic!("expected Basic pattern in WHERE");
        }
    }

    #[test]
    fn test_construct_multi_template() {
        let q = p(r#"PREFIX ex: <http://example.org/>
CONSTRUCT {
  ?s ex:label ?name .
  ?s ex:type ex:Person
}
WHERE { ?s ex:name ?name }"#);
        if let QueryForm::Construct { template } = &q.form {
            assert_eq!(template.len(), 2);
        } else {
            panic!("expected Construct");
        }
    }

    #[test]
    fn test_construct_with_semicolon_shorthand() {
        let q = p(r#"PREFIX ex: <http://example.org/>
CONSTRUCT { ?s ex:a ?o ; ex:b ?o2 }
WHERE { ?s ex:x ?o . ?s ex:y ?o2 }"#);
        if let QueryForm::Construct { template } = &q.form {
            assert_eq!(template.len(), 2);
            // Both should share the same subject ?s
            assert!(matches!(&template[0].subject, VarOrTerm::Variable(v) if v.name == "s"));
            assert!(matches!(&template[1].subject, VarOrTerm::Variable(v) if v.name == "s"));
        } else {
            panic!("expected Construct");
        }
    }

    #[test]
    fn test_construct_empty_template() {
        let q = p("CONSTRUCT { } WHERE { ?s ?p ?o }");
        if let QueryForm::Construct { template } = &q.form {
            assert_eq!(template.len(), 0);
        } else {
            panic!("expected Construct");
        }
    }

    #[test]
    fn test_construct_where_shortform() {
        // CONSTRUCT WHERE { pattern } — template equals the WHERE pattern
        let q = p("CONSTRUCT WHERE { ?s ?p ?o }");
        if let QueryForm::Construct { template } = &q.form {
            assert_eq!(template.len(), 1);
            assert!(matches!(&template[0].subject, VarOrTerm::Variable(v) if v.name == "s"));
            assert!(matches!(&template[0].predicate, VerbPath::Variable(v) if v.name == "p"));
            assert!(matches!(&template[0].object, VarOrTerm::Variable(v) if v.name == "o"));
        } else {
            panic!("expected Construct");
        }
        // WHERE clause should also have the same pattern
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert_eq!(triples.len(), 1);
        } else {
            panic!("expected Basic pattern in WHERE");
        }
    }

    #[test]
    fn test_construct_with_prefix() {
        let q = p(r#"PREFIX foaf: <http://xmlns.com/foaf/0.1/>
CONSTRUCT { ?person foaf:name ?name }
WHERE { ?person foaf:firstName ?name }"#);
        if let QueryForm::Construct { template } = &q.form {
            assert_eq!(template.len(), 1);
            if let VerbPath::Path(PropertyPath::Iri(iri)) = &template[0].predicate {
                assert_eq!(iri.value, "http://xmlns.com/foaf/0.1/name");
            } else {
                panic!("expected IRI predicate in template");
            }
        } else {
            panic!("expected Construct");
        }
    }

    #[test]
    fn test_construct_with_blank_node() {
        let q = p(r#"PREFIX ex: <http://example.org/>
CONSTRUCT { _:b ex:value ?v }
WHERE { ?s ex:data ?v }"#);
        if let QueryForm::Construct { template } = &q.form {
            assert_eq!(template.len(), 1);
            assert!(matches!(
                &template[0].subject,
                VarOrTerm::Term(GraphTerm::BlankNode(label)) if label == "b"
            ));
        } else {
            panic!("expected Construct");
        }
    }

    #[test]
    fn test_construct_with_literal_object() {
        let q = p(r#"PREFIX ex: <http://example.org/>
CONSTRUCT { ?s ex:status "active" }
WHERE { ?s ex:enabled true }"#);
        if let QueryForm::Construct { template } = &q.form {
            assert_eq!(template.len(), 1);
            assert!(matches!(
                &template[0].object,
                VarOrTerm::Term(GraphTerm::Literal(RdfLiteral::Simple(s))) if s == "active"
            ));
        } else {
            panic!("expected Construct");
        }
    }

    // === ASK tests ===

    #[test]
    fn test_ask_basic() {
        let q = p("ASK { ?s ?p ?o }");
        assert!(matches!(q.form, QueryForm::Ask));
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert_eq!(triples.len(), 1);
        } else {
            panic!("expected Basic pattern");
        }
    }

    #[test]
    fn test_ask_with_prefix() {
        let q = p(r#"PREFIX foaf: <http://xmlns.com/foaf/0.1/>
ASK { ?x foaf:name "Alice" }"#);
        assert!(matches!(q.form, QueryForm::Ask));
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert_eq!(triples.len(), 1);
        }
    }

    #[test]
    fn test_ask_with_where_keyword() {
        let q = p("ASK WHERE { ?s ?p ?o }");
        assert!(matches!(q.form, QueryForm::Ask));
    }

    #[test]
    fn test_ask_with_filter() {
        let q = p(r#"PREFIX ex: <http://example.org/>
ASK { ?s ex:age ?age . FILTER(?age > 18) }"#);
        assert!(matches!(q.form, QueryForm::Ask));
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            assert_eq!(patterns.len(), 2);
            assert!(matches!(&patterns[0], GroupGraphPattern::Basic(_)));
            assert!(matches!(&patterns[1], GroupGraphPattern::Filter(_)));
        } else {
            panic!("expected Group pattern");
        }
    }

    #[test]
    fn test_ask_with_optional() {
        let q = p(r#"PREFIX ex: <http://example.org/>
ASK {
  ?s ex:name ?name .
  OPTIONAL { ?s ex:email ?email }
}"#);
        assert!(matches!(q.form, QueryForm::Ask));
        if let GroupGraphPattern::Group(patterns) = &q.where_clause {
            assert_eq!(patterns.len(), 2);
        }
    }

    // === DESCRIBE tests ===

    #[test]
    fn test_describe_single_iri() {
        let q = p("DESCRIBE <http://example.org/Alice>");
        if let QueryForm::Describe { resources } = &q.form {
            assert_eq!(resources.len(), 1);
            assert!(matches!(
                &resources[0],
                VarOrIri::Iri(iri) if iri.value == "http://example.org/Alice"
            ));
        } else {
            panic!("expected Describe");
        }
        // No WHERE clause → empty Basic
        assert!(matches!(q.where_clause, GroupGraphPattern::Basic(ref t) if t.is_empty()));
    }

    #[test]
    fn test_describe_variable() {
        let q = p("DESCRIBE ?x WHERE { ?x ?p ?o }");
        if let QueryForm::Describe { resources } = &q.form {
            assert_eq!(resources.len(), 1);
            assert!(matches!(&resources[0], VarOrIri::Variable(v) if v.name == "x"));
        } else {
            panic!("expected Describe");
        }
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert_eq!(triples.len(), 1);
        }
    }

    #[test]
    fn test_describe_multiple_resources() {
        let q = p(r#"PREFIX ex: <http://example.org/>
DESCRIBE ?x ex:Alice ex:Bob"#);
        if let QueryForm::Describe { resources } = &q.form {
            assert_eq!(resources.len(), 3);
            assert!(matches!(&resources[0], VarOrIri::Variable(v) if v.name == "x"));
            assert!(
                matches!(&resources[1], VarOrIri::Iri(iri) if iri.value == "http://example.org/Alice")
            );
            assert!(
                matches!(&resources[2], VarOrIri::Iri(iri) if iri.value == "http://example.org/Bob")
            );
        } else {
            panic!("expected Describe");
        }
    }

    #[test]
    fn test_describe_star() {
        let q = p("DESCRIBE * WHERE { ?s ?p ?o }");
        if let QueryForm::Describe { resources } = &q.form {
            assert!(
                resources.is_empty(),
                "DESCRIBE * should have empty resources"
            );
        } else {
            panic!("expected Describe");
        }
    }

    #[test]
    fn test_describe_with_where() {
        let q = p(r#"PREFIX foaf: <http://xmlns.com/foaf/0.1/>
DESCRIBE ?person WHERE { ?person foaf:name "Alice" }"#);
        if let QueryForm::Describe { resources } = &q.form {
            assert_eq!(resources.len(), 1);
        }
        if let GroupGraphPattern::Basic(triples) = &q.where_clause {
            assert_eq!(triples.len(), 1);
        }
    }

    #[test]
    fn test_describe_no_where() {
        let q = p("DESCRIBE <http://example.org/thing>");
        assert!(matches!(q.form, QueryForm::Describe { .. }));
        assert!(matches!(q.where_clause, GroupGraphPattern::Basic(ref t) if t.is_empty()));
    }

    // === Error cases for new query forms ===

    #[test]
    fn test_error_expected_query_form() {
        let err = parse("FOOBAR { ?s ?p ?o }").unwrap_err();
        assert!(
            err.message.contains("SELECT, CONSTRUCT, ASK, or DESCRIBE"),
            "got: {}",
            err.message
        );
    }

    #[test]
    fn test_error_describe_empty() {
        let err = parse("DESCRIBE").unwrap_err();
        assert!(
            err.message.contains("variable, IRI, or '*' after DESCRIBE"),
            "got: {}",
            err.message
        );
    }

    #[test]
    fn test_error_construct_missing_brace() {
        let err = parse("CONSTRUCT ?s ?p ?o WHERE { ?s ?p ?o }").unwrap_err();
        assert!(err.message.contains("'{'"), "got: {}", err.message);
    }
}
