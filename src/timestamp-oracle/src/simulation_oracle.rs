use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use mz_repr::Timestamp;

use crate::{GenericNowFn, TimestampOracle, WriteTimestamp};

#[derive(Debug)]
pub struct SimulationTimestampOracle<N> {
    read_ts: Arc<Mutex<Timestamp>>,
    write_ts: Arc<Mutex<Timestamp>>,
    next: N,
}

impl<N> SimulationTimestampOracle<N> {
    pub fn new(initially: Timestamp, next: N) -> Self {
        Self {
            read_ts: Arc::new(Mutex::new(initially)),
            write_ts: Arc::new(Mutex::new(initially)),
            next,
        }
    }
}

#[async_trait]
impl<N> TimestampOracle<Timestamp> for SimulationTimestampOracle<N>
where
    N: GenericNowFn<Timestamp> + std::fmt::Debug + 'static,
{
    async fn write_ts(&self) -> WriteTimestamp<Timestamp> {
        let mut write_ts = self.write_ts.lock().unwrap();
        let mut next = self.next.now();
        if u64::from(next) <= u64::from(*write_ts) {
            next = write_ts.step_forward();
        }
        *write_ts = next;
        let advance_to = next.step_forward();
        WriteTimestamp {
            timestamp: next,
            advance_to,
        }
    }

    async fn peek_write_ts(&self) -> Timestamp {
        *self.write_ts.lock().unwrap()
    }

    async fn read_ts(&self) -> Timestamp {
        *self.read_ts.lock().unwrap()
    }

    async fn apply_write(&self, write_ts: Timestamp) {
        let mut read_ts = self.read_ts.lock().unwrap();
        if u64::from(*read_ts) < u64::from(write_ts) {
            *read_ts = write_ts;

            let mut write_ts = self.write_ts.lock().unwrap();
            if u64::from(*write_ts) < u64::from(*read_ts) {
                *write_ts = *read_ts;
            }
        }
    }
}
