//! A stream that holds what a pass sends and writes it in one go: the
//! send shape of a load generator that frames a connection's whole free
//! window into one buffer, rather than a write, and with Nagle off a
//! segment, per request.

use std::io;

use melin_client::ByteStream;

/// The least a stream's buffer holds, whatever the window: room for the
/// handshake's challenge response in one piece, which goes through the
/// same buffer, when a window's worth of requests is smaller. What a
/// partial write leaves behind needs no room of its own: those bytes are
/// requests already counted in the window.
const MIN_CAPACITY: usize = 64 * 1024;

/// `inner`, with every byte sent to it held until [`write`](Self::write).
///
/// Every send goes through the buffer, the plain one included: a
/// connection flushes what it queued earlier through the plain send before
/// framing a request in place, and a plain send that bypassed the buffer
/// would overtake the requests still in it.
///
/// Its owner writes it: [`service`](ByteStream::service) only services
/// the inner stream. So it is no stream for a driver that knows only
/// `service`, such as `StreamConnection`, which would wait on a full
/// buffer forever.
pub struct Batched<S> {
    inner: S,
    /// What is held, at `buf[head..tail]`; the region past `tail` is lent
    /// to a connection to frame into. A fixed-size boxed slice rather than
    /// a `Vec`: the capacity is the backpressure, and a full buffer refuses
    /// a send rather than growing.
    buf: Box<[u8]>,
    head: usize,
    tail: usize,
}

impl<S: ByteStream> Batched<S> {
    /// `inner`, holding up to `window` requests of `frame_len` bytes, and
    /// never less than [`MIN_CAPACITY`].
    pub fn new(inner: S, window: usize, frame_len: usize) -> Self {
        let capacity = window.saturating_mul(frame_len).max(MIN_CAPACITY);
        Self {
            inner,
            buf: vec![0; capacity].into_boxed_slice(),
            head: 0,
            tail: 0,
        }
    }

    /// Hand `inner` what is held, as much as it takes now, in one send when
    /// it takes it all. What it does not take stays held, in order, for
    /// the next write.
    pub fn write(&mut self) -> io::Result<()> {
        while self.head < self.tail {
            let n = self.inner.send(&self.buf[self.head..self.tail])?;
            if n == 0 {
                break;
            }
            self.head += n;
        }
        if self.head == self.tail {
            self.head = 0;
            self.tail = 0;
        }
        Ok(())
    }

    /// The free region behind what is held, as long as it can be: what a
    /// partial write left is moved to the front first.
    fn free_region(&mut self) -> &mut [u8] {
        if self.head > 0 {
            self.buf.copy_within(self.head..self.tail, 0);
            self.tail -= self.head;
            self.head = 0;
        }
        &mut self.buf[self.tail..]
    }
}

impl<S> Batched<S> {
    /// Bytes held, not yet written.
    pub fn held(&self) -> usize {
        self.tail - self.head
    }

    /// The stream under the buffer.
    #[cfg(test)]
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }
}

// By hand: the derived form would print the whole buffer.
impl<S: std::fmt::Debug> std::fmt::Debug for Batched<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Batched")
            .field("inner", &self.inner)
            .field("capacity", &self.buf.len())
            .field("held", &self.held())
            .finish()
    }
}

impl<S: ByteStream> ByteStream for Batched<S> {
    fn send(&mut self, data: &[u8]) -> io::Result<usize> {
        let region = self.free_region();
        let n = data.len().min(region.len());
        region[..n].copy_from_slice(&data[..n]);
        self.tail += n;
        Ok(n)
    }

    fn service(&mut self, now_ns: u64) {
        self.inner.service(now_ns);
    }

    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.recv(buf)
    }

    fn send_in_place(&mut self, fill: impl FnOnce(&mut [u8]) -> usize) -> io::Result<usize> {
        let region = self.free_region();
        if region.is_empty() {
            return Ok(0);
        }
        let len = region.len();
        let n = fill(region);
        if n > len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{n} bytes written into a {len}-byte region"),
            ));
        }
        self.tail += n;
        Ok(n)
    }
}

/// A stream for tests that counts its writes.
#[cfg(test)]
pub mod testing {
    use std::io;

    use melin_client::{BufferStream, ByteStream};

    /// A `BufferStream` that counts the sends it is given.
    #[derive(Debug, Default)]
    pub struct Counting {
        pub stream: BufferStream,
        pub sends: usize,
    }

    impl ByteStream for Counting {
        fn send(&mut self, data: &[u8]) -> io::Result<usize> {
            self.sends += 1;
            self.stream.send(data)
        }

        fn service(&mut self, now_ns: u64) {
            self.stream.service(now_ns);
        }

        fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.stream.recv(buf)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::Counting;
    use super::*;

    fn frame(stream: &mut Batched<Counting>, byte: u8, len: usize) {
        let n = stream
            .send_in_place(|region| {
                region[..len].fill(byte);
                len
            })
            .unwrap();
        assert_eq!(n, len);
    }

    #[test]
    fn a_window_framed_in_place_leaves_in_one_send() {
        let mut stream = Batched::new(Counting::default(), 4, 16);
        for byte in 1..=4 {
            frame(&mut stream, byte, 16);
        }
        assert_eq!(stream.inner.sends, 0, "nothing leaves before the write");
        stream.write().unwrap();
        assert_eq!(stream.inner.sends, 1);
        let expected: Vec<u8> = (1..=4).flat_map(|b| [b; 16]).collect();
        assert_eq!(stream.inner.stream.sent(), expected);

        // Nothing held: a write sends nothing.
        stream.write().unwrap();
        assert_eq!(stream.inner.sends, 1);
    }

    #[test]
    fn a_plain_send_queues_behind_what_is_held() {
        let mut stream = Batched::new(Counting::default(), 1, 8);
        frame(&mut stream, 1, 8);
        assert_eq!(stream.send(&[2; 8]).unwrap(), 8);
        frame(&mut stream, 3, 8);
        stream.write().unwrap();
        let expected: Vec<u8> = [[1; 8], [2; 8], [3; 8]].concat();
        assert_eq!(stream.inner.stream.sent(), expected);
    }

    #[test]
    fn what_a_partial_write_leaves_goes_next_in_order() {
        let mut stream = Batched::new(Counting::default(), 1, 8);
        stream.inner.stream.set_send_capacity(Some(10));
        frame(&mut stream, 1, 8);
        frame(&mut stream, 2, 8);
        stream.write().unwrap();
        assert_eq!(stream.inner.stream.sent().len(), 10);

        // More framed behind the rest, then the socket drains.
        frame(&mut stream, 3, 8);
        stream.inner.stream.consume_sent(10);
        stream.inner.stream.set_send_capacity(None);
        stream.write().unwrap();
        let expected: Vec<u8> = [&[2; 6][..], &[3; 8]].concat();
        assert_eq!(stream.inner.stream.sent(), expected);
    }

    #[test]
    fn a_full_buffer_refuses_rather_than_grows() {
        let mut stream = Batched::new(Counting::default(), 1, 8);
        stream.inner.stream.set_send_capacity(Some(0));
        let capacity = stream.buf.len();
        assert_eq!(stream.send(&vec![7; capacity + 5]).unwrap(), capacity);
        assert_eq!(stream.send(&[7]).unwrap(), 0);
        assert_eq!(
            stream
                .send_in_place(|_| unreachable!("no region to lend"))
                .unwrap(),
            0
        );
        stream.write().unwrap();
        assert_eq!(stream.buf.len(), capacity);
    }

    #[test]
    fn a_fill_past_its_region_is_refused_and_queues_nothing() {
        let mut stream = Batched::new(Counting::default(), 1, 8);
        let capacity = stream.buf.len();
        let err = stream.send_in_place(|_| capacity + 1).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        stream.write().unwrap();
        assert_eq!(stream.inner.sends, 0);
    }

    #[test]
    fn the_buffer_holds_a_full_window_however_wide() {
        let stream = Batched::new(Counting::default(), 1024, 512);
        assert_eq!(stream.buf.len(), 1024 * 512);
        let stream = Batched::new(Counting::default(), 1, 16);
        assert_eq!(stream.buf.len(), MIN_CAPACITY);
    }

    #[test]
    fn receives_and_a_close_come_from_the_inner_stream() {
        let mut stream = Batched::new(Counting::default(), 1, 8);
        stream.inner.stream.push_received(b"abc");
        stream.inner.stream.close();
        let mut buf = [0; 8];
        assert_eq!(stream.recv(&mut buf).unwrap(), 3);
        assert_eq!(&buf[..3], b"abc");
        let err = stream.recv(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }
}
