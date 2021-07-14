use async_trait::async_trait;
use futures::{stream::BoxStream, Stream};
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;

use repr::{RelationDesc, Timestamp};

use crate::{Error, Event};

/// The trait that defines a new kind of source.
#[async_trait]
pub trait Source: Serialize + for<'de> Deserialize<'de> {
    /// Type holding any parameters than can be specified using SQL keywords.
    /// For example CREATE SOURCE FROM <name> HOST 'example.com'` would
    /// correspond to `Sql` having a field `host` of type `String`.
    ///
    /// Types that implement `Deserialize` automatically implement `Deserialize`
    type Sql: for<'de> Deserialize<'de>;

    /// Type used to construct instances of this source
    type Config: InstanceConfig;

    /// The name of the source
    fn name(&self) -> &'static str;

    /// Constructs a new source from the parsed SQL. This is an opportunity for the implementation
    /// to do any sanity check of the parameters passed
    fn from_sql(sql: Self::Sql) -> Result<Self, Error>;

    /// An opportunity for the source to remove any impurities from its definition. This will be
    /// called before permanently storing the source in the catalog
    async fn purify(&mut self) { }

    /// Creates at most `workers` instance configs. These configs will be
    /// persisted in the catalog and be used to do any associated cleanup later.
    /// It is up to the system to decide how the returned configs will be
    /// distributed among the dataflow workers and converted into instances
    fn create_instances(&self, workers: usize) -> Vec<Self::Config>;

    /// The schema that will be produced by this source
    fn desc(&self) -> RelationDesc;

    /// If true, the source will only be available behind the experimental flag
    fn is_experimental(&self) -> bool {
        false
    }

    /// If false, the source will not be available in the cloud product
    fn is_safe(&self) -> bool {
        true
    }
}

#[async_trait]
pub trait InstanceConfig: Serialize + for<'de> Deserialize<'de> {
    type State: Default;
    /// The actual instance type that will produce data into the dataflow from a
    /// particular worker
    type Instance: Stream<Item = Event> + Send;

    /// Creates a source instance given its configuration
    fn instantiate(self, state: &mut Self::State) -> Self::Instance;

    /// Computes the timestamp beyond which this instance and `other` will
    /// produce identical updates. Data not beyond this timestamp might have been
    /// subjected to logical compaction.
    /// If this function returns None, the two instances will never converge and
    /// the user should be warned
    fn meet(&self, _other: &Self) -> Option<Antichain<Timestamp>> {
        None
    }

    /// Optional cleanup logic to run when an instance config is dropped. The
    /// default implementation simply is a no-op
    async fn cleanup(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

pub trait SimpleSource: Serialize + for<'de> Deserialize<'de> + Clone {
    /// The name of this source
    fn name(&self) -> &'static str;
    fn into_stream(self) -> BoxStream<'static, Event>;
    /// The schema that will be produced by this source
    fn desc(&self) -> RelationDesc;
}

impl<S: SimpleSource> Source for S {
    type Sql = Self;
    type Config = Self;

    fn name(&self) -> &'static str {
        self.name()
    }

    fn from_sql(sql: Self::Sql) -> Result<Self, Error> {
        Ok(sql)
    }

    fn create_instances(&self, _workers: usize) -> Vec<Self::Config> {
        vec![self.clone()]
    }

    fn desc(&self) -> RelationDesc {
        self.desc()
    }
}

impl<S: SimpleSource> InstanceConfig for S {
    type State = ();
    type Instance = BoxStream<'static, Event>;

    /// Creates a source instance given its configuration
    fn instantiate(self, _state: &mut Self::State) -> Self::Instance {
        self.into_stream()
    }
}
