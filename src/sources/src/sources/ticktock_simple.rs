use std::time::Duration;

use futures::stream::{BoxStream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::IntervalStream;

use crate::{Event, SimpleSource};
use repr::{Datum, RelationDesc, Row, ScalarType};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub struct TickTockSimple {
    millis: u64,
}

impl SimpleSource for TickTockSimple {
    fn name(&self) -> &'static str {
        "ticktocksimple"
    }

    fn into_stream(self) -> BoxStream<'static, Event> {
        IntervalStream::new(tokio::time::interval(Duration::from_millis(self.millis)))
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

    fn desc(&self) -> RelationDesc {
        RelationDesc::empty().with_named_column("id", ScalarType::Int64.nullable(false))
    }
}
