use std::pin::Pin;
use std::time::Duration;

use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::IntervalStream;

use crate::{Error, Event, InstanceConfig, Source};
use repr::{Datum, RelationDesc, Row, ScalarType};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TickTockSql {
    Millis(u64),
    Seconds(u64),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct TickTock {
    duration: Duration,
}

impl Source for TickTock {
    type Sql = TickTockSql;
    type Config = Self;

    fn name(&self) -> &'static str {
        "ticktock"
    }

    fn from_sql(sql: Self::Sql) -> Result<Self, Error> {
        let duration = match sql {
            Self::Sql::Millis(millis) => Duration::from_millis(millis),
            Self::Sql::Seconds(secs) => Duration::from_secs(secs),
        };

        // Validate that we have a positive duration
        if duration.as_nanos() == 0 {
            Err(())
        } else {
            Ok(Self { duration })
        }
    }

    fn create_instances(&self, _workers: usize) -> Vec<Self::Config> {
        vec![self.clone()]
    }

    fn desc(&self) -> RelationDesc {
        RelationDesc::empty().with_named_column("id", ScalarType::Int64.nullable(false))
    }
}

impl InstanceConfig for TickTock {
    type State = ();
    type Instance = Pin<Box<dyn Stream<Item = Event> + Send>>;

    fn instantiate(self, _state: &mut Self::State) -> Self::Instance {
        IntervalStream::new(tokio::time::interval(self.duration))
            .enumerate()
            .flat_map(|(ts, _)| {
                let mut row = Row::default();
                let datum: Datum = (ts as i64).into();
                row.push(datum);
                futures::stream::iter(std::array::IntoIter::new([
                    Event::Message(ts as u64, Ok((row, ts as u64, 1))),
                    Event::Progress(Some(ts as u64)),
                ]))
            })
            .boxed()
    }
}
