//! Client-side secret resolution for mz-deploy.
//!
//! Secret values in SQL files may reference client-side providers like `env_var('MY_VAR')`
//! instead of inline string literals. This module resolves those references at execution
//! time (not compile time), so `mz-deploy compile` works without access to secrets.
//!
//! Unknown functions and other expressions pass through unchanged to Materialize.
//!
//! ## Providers
//!
//! Each provider is a submodule that implements [`SecretProvider`]:
//!
//! - `env_var::EnvVarProvider` — reads from environment variables
//! - `aws_secret::AwsSecretProvider` — reads from AWS Secrets Manager
//! - `aws_secret::UnconfiguredAwsProvider` — placeholder when `aws_profile` is not set

mod aws_secret;
mod env_var;

use crate::cli::CliError;
use crate::config::SecurityConfig;
use crate::project::ast::Statement;
use async_trait::async_trait;
use aws_secret::{AwsSecretProvider, UnconfiguredAwsProvider};
use env_var::EnvVarProvider;
use mz_sql_parser::ast::{CreateSecretStatement, Expr, FunctionArgs, Raw, RawItemName, Value};
use std::collections::BTreeMap;
use thiserror::Error;

/// Errors that can occur during secret resolution.
#[derive(Debug, Error)]
pub enum SecretResolveError {
    /// A known provider was called with the wrong number of arguments.
    #[error("function '{name}' expects {expected} argument(s), got {got}")]
    WrongArgCount {
        name: String,
        expected: usize,
        got: usize,
    },
    /// A known provider was called with a non-literal argument.
    #[error("function '{name}' requires string literal arguments")]
    NonLiteralArg { name: String },
    /// A known provider failed to resolve.
    #[error("failed to resolve '{name}': {reason}")]
    ResolutionFailed { name: String, reason: String },
}

/// A provider that can resolve secret values from an external source.
#[async_trait]
pub trait SecretProvider: Send + Sync {
    /// The function name this provider handles (e.g. `"env_var"`).
    fn name(&self) -> &str;
    /// The number of arguments this provider expects.
    fn expected_args(&self) -> usize;
    /// Resolve the secret value from the given arguments.
    async fn resolve(&self, args: &[String]) -> Result<String, SecretResolveError>;
}

/// Resolves client-side secret provider functions in SQL expressions.
///
/// Known providers (like `env_var`) are resolved to string literals.
/// Unknown functions and other expressions pass through unchanged to Materialize.
pub struct SecretResolver {
    providers: BTreeMap<String, Box<dyn SecretProvider>>,
}

impl SecretResolver {
    /// Creates a new resolver with providers configured from the given AWS profile.
    ///
    /// Always registers `env_var`. If `aws_profile` is `Some`, registers the
    /// real AWS Secrets Manager provider (credentials are loaded lazily on
    /// first use); otherwise registers a placeholder that gives a clear error.
    pub fn new(config: &SecurityConfig) -> Self {
        let mut resolver = Self {
            providers: BTreeMap::new(),
        };
        resolver.register(Box::new(EnvVarProvider));

        if let Some(profile) = config.aws_profile() {
            resolver.register(Box::new(AwsSecretProvider::new(profile)));
        } else {
            resolver.register(Box::new(UnconfiguredAwsProvider));
        }

        resolver
    }

    fn register(&mut self, provider: Box<dyn SecretProvider>) {
        self.providers.insert(provider.name().to_string(), provider);
    }

    /// Resolve client-side provider functions in an expression.
    ///
    /// - `Expr::Function` matching a registered provider: validate and resolve to `Expr::Value(Value::String(...))`
    /// - Everything else: pass through unchanged
    pub async fn resolve_expr(&self, expr: Expr<Raw>) -> Result<Expr<Raw>, SecretResolveError> {
        match &expr {
            Expr::Function(func) => {
                let func_name = match &func.name {
                    RawItemName::Name(name) if name.0.len() == 1 => {
                        Some(name.0[0].as_str().to_string())
                    }
                    _ => None,
                };

                let func_name = match func_name {
                    Some(name) => name,
                    None => return Ok(expr),
                };

                let provider = match self.providers.get(&func_name) {
                    Some(p) => p,
                    None => return Ok(expr),
                };

                // Validate args
                let arg_exprs = match &func.args {
                    FunctionArgs::Star => {
                        return Err(SecretResolveError::WrongArgCount {
                            name: func_name,
                            expected: provider.expected_args(),
                            got: 0,
                        });
                    }
                    FunctionArgs::Args { args, .. } => args,
                };

                if arg_exprs.len() != provider.expected_args() {
                    return Err(SecretResolveError::WrongArgCount {
                        name: func_name,
                        expected: provider.expected_args(),
                        got: arg_exprs.len(),
                    });
                }

                let mut string_args = Vec::with_capacity(arg_exprs.len());
                for arg in arg_exprs {
                    match arg {
                        Expr::Value(Value::String(s)) => string_args.push(s.clone()),
                        _ => {
                            return Err(SecretResolveError::NonLiteralArg { name: func_name });
                        }
                    }
                }

                let resolved = provider.resolve(&string_args).await?;
                Ok(Expr::Value(Value::String(resolved)))
            }
            _ => Ok(expr),
        }
    }

    /// Resolves client-side provider functions in a `CREATE SECRET` statement.
    ///
    /// Returns a new statement with the value field resolved.
    pub async fn resolve_create_secret(
        &self,
        stmt: &CreateSecretStatement<Raw>,
    ) -> Result<CreateSecretStatement<Raw>, SecretResolveError> {
        let mut resolved = stmt.clone();
        resolved.value = self.resolve_expr(stmt.value.clone()).await?;
        Ok(resolved)
    }

    /// Resolves a `CREATE SECRET` statement and maps errors to [`CliError`].
    pub async fn resolve_secret_for_cli(
        &self,
        stmt: &CreateSecretStatement<Raw>,
    ) -> Result<CreateSecretStatement<Raw>, CliError> {
        self.resolve_create_secret(stmt)
            .await
            .map_err(|e| CliError::SecretResolution {
                secret_name: stmt.name.to_string(),
                source: e,
            })
    }

    /// Resolves a [`Statement`], returning the resolved version.
    ///
    /// Secret statements have their provider functions resolved;
    /// all other statements are returned unchanged.
    pub async fn resolve_statement_for_cli(&self, stmt: &Statement) -> Result<Statement, CliError> {
        match stmt {
            Statement::CreateSecret(create_stmt) => {
                let resolved = self.resolve_secret_for_cli(create_stmt).await?;
                Ok(Statement::CreateSecret(resolved))
            }
            _ => Ok(stmt.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_sql_parser::ast::{Function, Ident, UnresolvedItemName};

    /// Helper to build `env_var('var_name')` as an `Expr<Raw>`.
    fn make_env_var_expr(var_name: &str) -> Expr<Raw> {
        Expr::Function(Function {
            name: RawItemName::Name(UnresolvedItemName(vec![Ident::new("env_var").unwrap()])),
            args: FunctionArgs::Args {
                args: vec![Expr::Value(Value::String(var_name.to_string()))],
                order_by: vec![],
            },
            filter: None,
            over: None,
            distinct: false,
        })
    }

    fn make_function_expr(name: &str, args: Vec<Expr<Raw>>) -> Expr<Raw> {
        Expr::Function(Function {
            name: RawItemName::Name(UnresolvedItemName(vec![Ident::new(name).unwrap()])),
            args: FunctionArgs::Args {
                args,
                order_by: vec![],
            },
            filter: None,
            over: None,
            distinct: false,
        })
    }

    #[tokio::test]
    async fn test_resolve_string_literal_passthrough() {
        let resolver = SecretResolver::new(&Default::default());
        let expr = Expr::Value(Value::String("hello".to_string()));
        let original = format!("{}", expr);
        let resolved = resolver.resolve_expr(expr).await.unwrap();
        assert_eq!(format!("{}", resolved), original);
    }

    #[tokio::test]
    async fn test_resolve_env_var_success() {
        // SAFETY: test-only; no other thread reads this variable.
        unsafe { std::env::set_var("MZ_TEST_SECRET_123", "my_secret_value") };
        let resolver = SecretResolver::new(&Default::default());
        let expr = make_env_var_expr("MZ_TEST_SECRET_123");
        let resolved = resolver.resolve_expr(expr).await.unwrap();
        match resolved {
            Expr::Value(Value::String(s)) => assert_eq!(s, "my_secret_value"),
            other => panic!("expected string literal, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_resolve_env_var_not_set() {
        let resolver = SecretResolver::new(&Default::default());
        let expr = make_env_var_expr("MZ_DEFINITELY_NOT_SET_XYZ_999");
        let err = resolver.resolve_expr(expr).await.unwrap_err();
        assert!(matches!(err, SecretResolveError::ResolutionFailed { .. }));
    }

    #[tokio::test]
    async fn test_resolve_unknown_function_passthrough() {
        let resolver = SecretResolver::new(&Default::default());
        let expr = make_function_expr("vault", vec![Expr::Value(Value::String("foo".to_string()))]);
        let original = format!("{}", expr);
        let resolved = resolver.resolve_expr(expr).await.unwrap();
        assert_eq!(format!("{}", resolved), original);
    }

    #[tokio::test]
    async fn test_resolve_arbitrary_expr_passthrough() {
        let resolver = SecretResolver::new(&Default::default());

        // Number literal
        let expr = Expr::Value(Value::Number("42".to_string()));
        let resolved = resolver.resolve_expr(expr).await.unwrap();
        assert_eq!(format!("{}", resolved), "42");

        // Identifier
        let expr = Expr::Identifier(vec![Ident::new("some_col").unwrap()]);
        let original = format!("{}", expr);
        let resolved = resolver.resolve_expr(expr).await.unwrap();
        assert_eq!(format!("{}", resolved), original);
    }

    #[tokio::test]
    async fn test_resolve_wrong_arg_count_zero() {
        let resolver = SecretResolver::new(&Default::default());
        let expr = make_function_expr("env_var", vec![]);
        let err = resolver.resolve_expr(expr).await.unwrap_err();
        match err {
            SecretResolveError::WrongArgCount {
                name,
                expected,
                got,
            } => {
                assert_eq!(name, "env_var");
                assert_eq!(expected, 1);
                assert_eq!(got, 0);
            }
            other => panic!("expected WrongArgCount, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_resolve_wrong_arg_count_two() {
        let resolver = SecretResolver::new(&Default::default());
        let expr = make_function_expr(
            "env_var",
            vec![
                Expr::Value(Value::String("A".to_string())),
                Expr::Value(Value::String("B".to_string())),
            ],
        );
        let err = resolver.resolve_expr(expr).await.unwrap_err();
        match err {
            SecretResolveError::WrongArgCount {
                name,
                expected,
                got,
            } => {
                assert_eq!(name, "env_var");
                assert_eq!(expected, 1);
                assert_eq!(got, 2);
            }
            other => panic!("expected WrongArgCount, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_resolve_non_literal_arg() {
        let resolver = SecretResolver::new(&Default::default());
        let expr = make_function_expr(
            "env_var",
            vec![Expr::Identifier(vec![Ident::new("col").unwrap()])],
        );
        let err = resolver.resolve_expr(expr).await.unwrap_err();
        assert!(matches!(err, SecretResolveError::NonLiteralArg { .. }));
    }

    #[tokio::test]
    async fn test_resolve_star_args() {
        let resolver = SecretResolver::new(&Default::default());
        let expr = Expr::Function(Function {
            name: RawItemName::Name(UnresolvedItemName(vec![Ident::new("env_var").unwrap()])),
            args: FunctionArgs::Star,
            filter: None,
            over: None,
            distinct: false,
        });
        let err = resolver.resolve_expr(expr).await.unwrap_err();
        match err {
            SecretResolveError::WrongArgCount { got: 0, .. } => {}
            other => panic!("expected WrongArgCount with got=0, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_resolve_create_secret_with_env_var() {
        // SAFETY: test-only; no other thread reads this variable.
        unsafe { std::env::set_var("MZ_TEST_SECRET_456", "resolved_value") };
        let resolver = SecretResolver::new(&Default::default());
        let stmt = CreateSecretStatement::<Raw> {
            name: UnresolvedItemName(vec![Ident::new("my_secret").unwrap()]),
            if_not_exists: false,
            value: make_env_var_expr("MZ_TEST_SECRET_456"),
        };
        let resolved = resolver.resolve_create_secret(&stmt).await.unwrap();
        match &resolved.value {
            Expr::Value(Value::String(s)) => assert_eq!(s, "resolved_value"),
            other => panic!("expected string literal, got: {:?}", other),
        }
        assert_eq!(resolved.name.0[0].as_str(), stmt.name.0[0].as_str());
    }

    #[tokio::test]
    async fn test_resolve_create_secret_plain_string() {
        let resolver = SecretResolver::new(&Default::default());
        let stmt = CreateSecretStatement::<Raw> {
            name: UnresolvedItemName(vec![Ident::new("my_secret").unwrap()]),
            if_not_exists: false,
            value: Expr::Value(Value::String("plain_value".to_string())),
        };
        let resolved = resolver.resolve_create_secret(&stmt).await.unwrap();
        match &resolved.value {
            Expr::Value(Value::String(s)) => assert_eq!(s, "plain_value"),
            other => panic!("expected string literal, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_unconfigured_aws_provider_error() {
        let resolver = SecretResolver::new(&Default::default());
        let expr = make_function_expr("aws_secret", vec![Expr::Value(Value::String("foo".into()))]);
        let err = resolver.resolve_expr(expr).await.unwrap_err();
        match err {
            SecretResolveError::ResolutionFailed { name, reason } => {
                assert_eq!(name, "aws_secret");
                assert!(
                    reason.contains("aws_profile"),
                    "error should mention aws_profile, got: {}",
                    reason
                );
            }
            other => panic!("expected ResolutionFailed, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_aws_secret_passthrough_when_unconfigured() {
        // env_var still works even when AWS is unconfigured
        unsafe { std::env::set_var("MZ_TEST_AWS_PASSTHROUGH", "works") };
        let resolver = SecretResolver::new(&Default::default());
        let expr = make_env_var_expr("MZ_TEST_AWS_PASSTHROUGH");
        let resolved = resolver.resolve_expr(expr).await.unwrap();
        match resolved {
            Expr::Value(Value::String(s)) => assert_eq!(s, "works"),
            other => panic!("expected string literal, got: {:?}", other),
        }
    }
}
