use crate::{InstanceConfig, Source};

// Convenience type-level functions
pub(crate) type Sql<S> = <S as Source>::Sql;
pub(crate) type Config<S> = <S as Source>::Config;
pub(crate) type State<S> = <Config<S> as InstanceConfig>::State;
pub(crate) type Instance<S> = <Config<S> as InstanceConfig>::Instance;

/// This macro generates a synthesized source from a list of consistuents sources. It will generate
/// a `SomeSource` enum which is in spirit the same as a `dyn Source`, i.e other code in materialize
/// can treat this type as "some source" without knowing which one specifically it is.
///
/// The reason a concrete type is used and not a trait object is due to the `where Self: Sized`
/// requirements of some methods and due to the existence of associated types which make the traits
/// not object safe. On the flip side, dispatching based on a enum is much faster than going through
/// a vtable (based on `enum_dispatch`'s benchmarks).
///
/// Concretely, this macro generates:
/// `SomeSource`:  an enum representing any source
/// `SomeSql`:     an enum representing any SQL expression of any source. It is used for parsing
/// `SomeConfig`:  an enum representing the config of a source. It can be used to get `SomeInstance`
/// `SomeState`:   a struct that holds the state for creating sources. Contrary to the other items
///                which are enums, this one holds the state for *all* sources inside it and picks
///                the right one when `instantiate` is called. This makes it impossible to attempt
///                to instantiate `SomeConfig` of source A with state from source B. Since the types
///                are erased the only option in that case would be to either panic!() or make the
///                methods fallible. With this trick, we don't need to do any of that
/// `SomeInstance: an enum represneting a source instance. It used by dataflow to get data into it
#[macro_export]
macro_rules! sources {
    ($($source:ident),*) => {
        paste::paste! {
            #[derive(serde::Serialize, serde::Deserialize)]
            #[serde(rename_all = "lowercase")]
            pub enum SomeSql {
                $($source(crate::macros::Sql<crate::$source>)),*
            }

            #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Hash)]
            pub enum SomeSource {
                $($source(crate::$source)),*
            }

            impl Source for SomeSource {
                type Sql = SomeSql;
                type Config = SomeConfig;

                fn name(&self) -> &'static str {
                    match self {
                        $(Self::$source(src) => Source::name(src)),*
                    }
                }

                fn from_sql(sql: Self::Sql) -> Result<Self, Error> {
                    match sql {
                        $(SomeSql::$source(sql) => {
                            crate::$source::from_sql(sql).map(SomeSource::$source)
                        }),*
                    }
                }

                fn create_instances(&self, w: usize) -> Vec<Self::Config> {
                    match self {
                        $(Self::$source(src) => {
                            src.create_instances(w).into_iter().map(SomeConfig::$source).collect()
                        }),*
                    }
                }

                fn is_experimental(&self) -> bool {
                    match self {
                        $(Self::$source(src) => Source::is_experimental(src)),*
                    }
                }

                fn is_safe(&self) -> bool {
                    match self {
                        $(Self::$source(src) => Source::is_safe(src)),*
                    }
                }

                fn desc(&self) -> repr::RelationDesc {
                    match self {
                        $(Self::$source(src) => Source::desc(src)),*
                    }
                }
            }

            #[derive(Default)]
            pub struct SomeState {
                $([<$source:snake>]: crate::macros::State<crate::$source>),*
            }

            #[derive(serde::Serialize, serde::Deserialize)]
            pub enum SomeConfig {
                $($source(crate::macros::Config<crate::$source>)),*
            }

            #[async_trait::async_trait]
            impl crate::InstanceConfig for SomeConfig {
                type State = SomeState;
                type Instance = SomeInstance;

                fn instantiate(self, s: &mut Self::State) -> Self::Instance {
                    match self {
                        $(
                            Self::$source(c) => {
                                Self::Instance::$source(c.instantiate(&mut s.[<$source:snake>]))
                            }
                        ),*
                    }
                }

                fn meet(&self, other: &Self) -> Option<timely::progress::Antichain<Timestamp>> {
                    #[allow(unreachable_patterns)]
                    match (self, other) {
                        $((Self::$source(this), Self::$source(other)) => this.meet(other)),*,
                        _ => None
                    }
                }

                async fn cleanup(&mut self) -> Result<(), Error> {
                    match self {
                        $(Self::$source(inner) => inner.cleanup().await),*
                    }
                }
            }

            #[pin_project::pin_project(project = SomeInstanceProj)]
            pub enum SomeInstance {
                $($source(#[pin] crate::macros::Instance<crate::$source>)),*
            }

            impl futures::Stream for SomeInstance {
                type Item = crate::Event;

                fn poll_next(
                    self: std::pin::Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>
                ) -> std::task::Poll<Option<Self::Item>> {
                    match self.project() {
                        $(SomeInstanceProj::$source(inner) => inner.poll_next(cx)),*
                    }
                }
            }
        }
    }
}
