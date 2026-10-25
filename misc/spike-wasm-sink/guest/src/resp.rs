//! THROWAWAY minimal pipelined RESP2 client over `std::net::TcpStream`.

use std::io::{Read, Write};
use std::net::TcpStream;

#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Reply>>),
}

pub struct Conn {
    stream: TcpStream,
    out: Vec<u8>,
    inbuf: Vec<u8>,
    inpos: usize,
    /// Commands written but whose replies are not read yet.
    in_flight: usize,
}

impl Conn {
    pub fn connect(addr: &str) -> Result<Self, String> {
        let stream = TcpStream::connect(addr).map_err(|e| format!("connect {addr}: {e}"))?;
        let _ = stream.set_nodelay(true);
        Ok(Conn {
            stream,
            out: Vec::with_capacity(1 << 20),
            inbuf: Vec::with_capacity(1 << 16),
            inpos: 0,
            in_flight: 0,
        })
    }

    /// Queues a command. Nothing is sent until `flush` or `call`.
    pub fn queue<A: AsRef<[u8]>>(&mut self, args: &[A]) {
        self.out.extend_from_slice(b"*");
        self.out.extend_from_slice(args.len().to_string().as_bytes());
        self.out.extend_from_slice(b"\r\n");
        for a in args {
            let a = a.as_ref();
            self.out.extend_from_slice(b"$");
            self.out.extend_from_slice(a.len().to_string().as_bytes());
            self.out.extend_from_slice(b"\r\n");
            self.out.extend_from_slice(a);
            self.out.extend_from_slice(b"\r\n");
        }
        self.in_flight += 1;
    }

    pub fn queued(&self) -> usize {
        self.in_flight
    }

    /// Sends queued commands and reads all their replies. Returns the replies,
    /// failing on the first error reply.
    pub fn flush(&mut self) -> Result<Vec<Reply>, String> {
        if !self.out.is_empty() {
            self.stream.write_all(&self.out).map_err(|e| format!("write: {e}"))?;
            self.out.clear();
        }
        let mut replies = Vec::with_capacity(self.in_flight);
        while self.in_flight > 0 {
            let r = self.read_reply()?;
            self.in_flight -= 1;
            replies.push(r);
        }
        if let Some(Reply::Error(e)) = replies.iter().find(|r| matches!(r, Reply::Error(_))) {
            return Err(format!("valkey error: {e}"));
        }
        Ok(replies)
    }

    pub fn call<A: AsRef<[u8]>>(&mut self, args: &[A]) -> Result<Reply, String> {
        self.queue(args);
        let mut r = self.flush()?;
        Ok(r.pop().expect("one reply"))
    }

    fn fill(&mut self) -> Result<(), String> {
        if self.inpos > 0 && self.inpos == self.inbuf.len() {
            self.inbuf.clear();
            self.inpos = 0;
        }
        let mut tmp = [0u8; 65536];
        let n = self.stream.read(&mut tmp).map_err(|e| format!("read: {e}"))?;
        if n == 0 {
            return Err("connection closed".into());
        }
        self.inbuf.extend_from_slice(&tmp[..n]);
        Ok(())
    }

    fn line(&mut self) -> Result<Vec<u8>, String> {
        loop {
            if let Some(i) = self.inbuf[self.inpos..].windows(2).position(|w| w == b"\r\n") {
                let line = self.inbuf[self.inpos..self.inpos + i].to_vec();
                self.inpos += i + 2;
                return Ok(line);
            }
            self.fill()?;
        }
    }

    fn exact(&mut self, n: usize) -> Result<Vec<u8>, String> {
        while self.inbuf.len() - self.inpos < n + 2 {
            self.fill()?;
        }
        let v = self.inbuf[self.inpos..self.inpos + n].to_vec();
        self.inpos += n + 2;
        Ok(v)
    }

    fn read_reply(&mut self) -> Result<Reply, String> {
        let line = self.line()?;
        let (tag, rest) = line.split_first().ok_or("empty reply line")?;
        let rest = String::from_utf8_lossy(rest).into_owned();
        let num = || rest.parse::<i64>().map_err(|e| format!("bad int {rest:?}: {e}"));
        Ok(match tag {
            b'+' => Reply::Simple(rest),
            b'-' => Reply::Error(rest),
            b':' => Reply::Int(num()?),
            b'$' => {
                let n = num()?;
                if n < 0 {
                    Reply::Bulk(None)
                } else {
                    Reply::Bulk(Some(self.exact(usize::try_from(n).unwrap())?))
                }
            }
            b'*' => {
                let n = num()?;
                if n < 0 {
                    Reply::Array(None)
                } else {
                    let mut v = Vec::new();
                    for _ in 0..n {
                        v.push(self.read_reply()?);
                    }
                    Reply::Array(Some(v))
                }
            }
            t => return Err(format!("unknown reply tag {}", *t as char)),
        })
    }
}
