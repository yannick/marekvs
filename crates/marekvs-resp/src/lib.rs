//! RESP2/RESP3 protocol codec for marekvs.
//!
//! Pure protocol logic — no I/O, no async, std only.
//!
//! Two halves:
//!
//! * [`RespParser`] — an incremental request parser. Raw socket bytes are fed
//!   in via [`RespParser::feed`]; complete commands are pulled out with
//!   [`RespParser::next_command`]. It understands RESP multi-bulk arrays
//!   (`*N\r\n` then N bulk strings) and inline commands (a bare line split on
//!   ASCII whitespace, for telnet / health-check compatibility).
//! * [`ReplyBuf`] — a reply serializer that is RESP3-aware and applies the
//!   standard RESP2 downgrades automatically (map → flat array, set → array,
//!   `_` null → `$-1`, double → bulk string, and so on).

/// A protocol-level failure while parsing a client request.
#[derive(Debug, PartialEq, Eq)]
pub enum RespError {
    /// Malformed input. The connection should send the error and then close —
    /// the byte stream can no longer be resynchronized.
    Protocol(String),
    /// A bulk string or multi-bulk array exceeded the configured limits
    /// ([`MAX_BULK`] / [`MAX_ARGS`]).
    TooLarge,
}

/// Maximum size of a single bulk string in a request (512 MiB).
pub const MAX_BULK: usize = 512 * 1024 * 1024;

/// Maximum number of arguments in a single multi-bulk request.
pub const MAX_ARGS: usize = 1024 * 1024;

/// Compact the parser buffer once this many consumed bytes have accumulated.
const COMPACT_THRESHOLD: usize = 8192;

/// Incremental request parser. Feed raw socket bytes, pull complete commands.
///
/// The parser keeps unconsumed bytes in an internal buffer and re-scans from
/// the current head on each [`next_command`](Self::next_command) call, so
/// partial frames split across many [`feed`](Self::feed) calls parse
/// correctly. Consumed bytes are dropped from the buffer amortized (the head
/// cursor advances, and the buffer is compacted once enough has been consumed).
#[derive(Debug)]
pub struct RespParser {
    buf: Vec<u8>,
    head: usize,
}

impl RespParser {
    /// Create an empty parser.
    pub fn new() -> Self {
        RespParser {
            buf: Vec::new(),
            head: 0,
        }
    }

    /// Append incoming bytes to the internal buffer.
    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Try to parse the next complete command.
    ///
    /// * `Ok(Some(args))` — one complete command; `args[0]` is the command name
    ///   as raw bytes, followed by its arguments. The consumed bytes are
    ///   removed from the buffer.
    /// * `Ok(None)` — need more data; the buffer is left intact.
    /// * `Err(_)` — a protocol error; the connection should be closed.
    ///
    /// Empty inline lines and empty (`*0`) arrays carry no command and are
    /// skipped transparently.
    pub fn next_command(&mut self) -> Result<Option<Vec<Vec<u8>>>, RespError> {
        loop {
            match parse_command(&self.buf[self.head..])? {
                None => return Ok(None),
                Some((consumed, args)) => {
                    self.head += consumed;
                    self.maybe_compact();
                    if args.is_empty() {
                        // Empty inline line or *0 array: no command, keep going.
                        continue;
                    }
                    return Ok(Some(args));
                }
            }
        }
    }

    /// Bytes currently buffered but not yet consumed (for metrics/backpressure).
    pub fn buffered(&self) -> usize {
        self.buf.len() - self.head
    }

    fn maybe_compact(&mut self) {
        if self.head == self.buf.len() {
            self.buf.clear();
            self.head = 0;
        } else if self.head >= COMPACT_THRESHOLD {
            self.buf.drain(0..self.head);
            self.head = 0;
        }
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of one parse attempt: `Some((consumed, args))` on a complete command
/// (args may be empty for a skipped frame), or `None` if more bytes are needed.
type ParseStep = Result<Option<(usize, Vec<Vec<u8>>)>, RespError>;

/// Parse one command from the front of `data`.
///
/// Returns `Ok(Some((consumed, args)))` on a complete command (`args` may be
/// empty for a skipped empty inline line or a `*0` array), `Ok(None)` if more
/// bytes are needed, or `Err` on malformed input.
fn parse_command(data: &[u8]) -> ParseStep {
    if data.is_empty() {
        return Ok(None);
    }
    if data[0] == b'*' {
        parse_multibulk(data)
    } else {
        parse_inline(data)
    }
}

/// Parse an inline command: everything up to the next `\n`, split on ASCII
/// whitespace. Accepts either `\n` or `\r\n` line endings.
fn parse_inline(data: &[u8]) -> ParseStep {
    let Some(rel_nl) = data.iter().position(|&b| b == b'\n') else {
        return Ok(None);
    };
    let consumed = rel_nl + 1;
    // Strip a single trailing '\r' (CRLF) if present.
    let mut line_end = rel_nl;
    if line_end > 0 && data[line_end - 1] == b'\r' {
        line_end -= 1;
    }
    let line = &data[..line_end];
    let args: Vec<Vec<u8>> = line
        .split(|&b| b.is_ascii_whitespace())
        .filter(|tok| !tok.is_empty())
        .map(|tok| tok.to_vec())
        .collect();
    Ok(Some((consumed, args)))
}

/// Parse a RESP multi-bulk array of bulk strings: `*N\r\n` then N × `$len\r\n<bytes>\r\n`.
fn parse_multibulk(data: &[u8]) -> ParseStep {
    // data[0] == '*'
    let (num_end, mut pos) = match read_crlf_line(data, 1)? {
        Some(x) => x,
        None => return Ok(None),
    };
    let count = parse_int(&data[1..num_end])?;
    if count < 0 {
        // *-1 (null array) is not a valid request: clients don't send nulls.
        return Err(RespError::Protocol("invalid multibulk length".into()));
    }
    let count = count as usize;
    if count > MAX_ARGS {
        return Err(RespError::TooLarge);
    }

    let mut args: Vec<Vec<u8>> = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        if pos >= data.len() {
            return Ok(None);
        }
        if data[pos] != b'$' {
            return Err(RespError::Protocol(format!(
                "expected '$', got '{}'",
                data[pos] as char
            )));
        }
        let (len_end, len_next) = match read_crlf_line(data, pos + 1)? {
            Some(x) => x,
            None => return Ok(None),
        };
        let blen = parse_int(&data[pos + 1..len_end])?;
        if blen < 0 {
            // $-1 (null bulk) inside a request is invalid.
            return Err(RespError::Protocol("invalid bulk length".into()));
        }
        let blen = blen as usize;
        if blen > MAX_BULK {
            return Err(RespError::TooLarge);
        }
        let data_start = len_next;
        let data_end = data_start + blen;
        // Need the payload plus its trailing CRLF.
        if data_end + 2 > data.len() {
            return Ok(None);
        }
        if data[data_end] != b'\r' || data[data_end + 1] != b'\n' {
            return Err(RespError::Protocol("expected CRLF after bulk".into()));
        }
        args.push(data[data_start..data_end].to_vec());
        pos = data_end + 2;
    }
    Ok(Some((pos, args)))
}

/// Read a strict-CRLF-terminated line starting at `start`.
///
/// On success returns `(content_end, next)` where the line content is
/// `data[start..content_end]` and `next` is the index just past the `\n`.
/// A lone `\n` without a preceding `\r` is a protocol error (typed frames are
/// strict CRLF). Returns `Ok(None)` if no `\n` has arrived yet.
fn read_crlf_line(data: &[u8], start: usize) -> Result<Option<(usize, usize)>, RespError> {
    let Some(rel) = data[start..].iter().position(|&b| b == b'\n') else {
        return Ok(None);
    };
    let nl = start + rel;
    if nl == start || data[nl - 1] != b'\r' {
        return Err(RespError::Protocol("expected CRLF".into()));
    }
    Ok(Some((nl - 1, nl + 1)))
}

/// Parse a signed base-10 integer from ASCII digits (optional leading `-`).
fn parse_int(s: &[u8]) -> Result<i64, RespError> {
    let (neg, digits) = match s.first() {
        Some(b'-') => (true, &s[1..]),
        _ => (false, s),
    };
    if digits.is_empty() {
        return Err(RespError::Protocol("invalid integer".into()));
    }
    let mut val: i64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return Err(RespError::Protocol("invalid integer".into()));
        }
        val = val
            .checked_mul(10)
            .and_then(|v| v.checked_add((b - b'0') as i64))
            .ok_or_else(|| RespError::Protocol("integer overflow".into()))?;
    }
    Ok(if neg { -val } else { val })
}

/// Reply serializer. RESP3-aware with automatic RESP2 downgrades.
///
/// Set [`resp3`](Self::resp3) from the connection's negotiated protocol
/// (`HELLO`). Each method appends one frame to an internal buffer; drain the
/// accumulated bytes with [`take`](Self::take).
#[derive(Debug)]
pub struct ReplyBuf {
    /// Whether to emit native RESP3 frames. When `false`, RESP2 downgrades are
    /// applied automatically.
    pub resp3: bool,
    buf: Vec<u8>,
}

impl ReplyBuf {
    /// Create an empty reply buffer for the given protocol version.
    pub fn new(resp3: bool) -> Self {
        ReplyBuf {
            resp3,
            buf: Vec::new(),
        }
    }

    /// Simple string: `+s\r\n`.
    pub fn simple(&mut self, s: &str) {
        self.buf.push(b'+');
        self.buf.extend_from_slice(s.as_bytes());
        self.crlf();
    }

    /// Error: `-msg\r\n`. `msg` must already include the error code, e.g.
    /// `"ERR unknown command"`.
    pub fn error(&mut self, msg: &str) {
        self.buf.push(b'-');
        self.buf.extend_from_slice(msg.as_bytes());
        self.crlf();
    }

    /// Integer: `:i\r\n`.
    pub fn int(&mut self, i: i64) {
        self.buf.push(b':');
        self.write_i64(i);
        self.crlf();
    }

    /// Bulk string: `$len\r\n<bytes>\r\n`.
    pub fn bulk(&mut self, b: &[u8]) {
        self.header(b'$', b.len());
        self.buf.extend_from_slice(b);
        self.crlf();
    }

    /// Bulk string from a `&str` (convenience over [`bulk`](Self::bulk)).
    pub fn bulk_str(&mut self, s: &str) {
        self.bulk(s.as_bytes());
    }

    /// Null. RESP3: `_\r\n`. RESP2: `$-1\r\n`.
    pub fn null(&mut self) {
        if self.resp3 {
            self.buf.extend_from_slice(b"_\r\n");
        } else {
            self.buf.extend_from_slice(b"$-1\r\n");
        }
    }

    /// Null array. RESP3: `_\r\n`. RESP2: `*-1\r\n`.
    pub fn null_array(&mut self) {
        if self.resp3 {
            self.buf.extend_from_slice(b"_\r\n");
        } else {
            self.buf.extend_from_slice(b"*-1\r\n");
        }
    }

    /// Array header only: `*len\r\n`. Caller emits `len` elements after.
    pub fn array(&mut self, len: usize) {
        self.header(b'*', len);
    }

    /// Map header. RESP3: `%pairs\r\n`. RESP2: `*(2*pairs)\r\n` (flat array).
    /// Caller emits `2*pairs` elements (key, value, …) after.
    pub fn map(&mut self, pairs: usize) {
        if self.resp3 {
            self.header(b'%', pairs);
        } else {
            self.header(b'*', pairs * 2);
        }
    }

    /// Set header. RESP3: `~len\r\n`. RESP2: `*len\r\n`.
    pub fn set(&mut self, len: usize) {
        if self.resp3 {
            self.header(b'~', len);
        } else {
            self.header(b'*', len);
        }
    }

    /// Push header. RESP3: `>len\r\n`. RESP2: `*len\r\n`.
    pub fn push(&mut self, len: usize) {
        if self.resp3 {
            self.header(b'>', len);
        } else {
            self.header(b'*', len);
        }
    }

    /// Double. RESP3: `,f\r\n`. RESP2: bulk string of the same formatting.
    ///
    /// Formatted like Redis: no trailing `.0` for integral values, `inf` /
    /// `-inf` for infinities, `nan` for NaN.
    pub fn double(&mut self, f: f64) {
        let s = fmt_double(f);
        if self.resp3 {
            self.buf.push(b',');
            self.buf.extend_from_slice(s.as_bytes());
            self.crlf();
        } else {
            self.bulk_str(&s);
        }
    }

    /// Boolean. RESP3: `#t\r\n` / `#f\r\n`. RESP2: `:1\r\n` / `:0\r\n`.
    pub fn boolean(&mut self, b: bool) {
        if self.resp3 {
            self.buf
                .extend_from_slice(if b { b"#t\r\n" } else { b"#f\r\n" });
        } else {
            self.buf
                .extend_from_slice(if b { b":1\r\n" } else { b":0\r\n" });
        }
    }

    /// Verbatim string. RESP3: `=len\r\ntxt:s\r\n` (len counts the `txt:`
    /// prefix). RESP2: plain bulk string.
    pub fn verbatim(&mut self, s: &str) {
        if self.resp3 {
            self.header(b'=', s.len() + 4);
            self.buf.extend_from_slice(b"txt:");
            self.buf.extend_from_slice(s.as_bytes());
            self.crlf();
        } else {
            self.bulk_str(s);
        }
    }

    /// Whether nothing has been serialized yet.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Bytes serialized so far.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Drain and return everything serialized so far, leaving the buffer empty.
    pub fn take(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buf)
    }

    /// Append pre-serialized RESP bytes (e.g. replies produced by a
    /// concurrent dispatcher) verbatim.
    pub fn extend_raw(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Borrow the bytes serialized so far without draining.
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    fn header(&mut self, prefix: u8, n: usize) {
        self.buf.push(prefix);
        self.write_usize(n);
        self.crlf();
    }

    fn crlf(&mut self) {
        self.buf.extend_from_slice(b"\r\n");
    }

    fn write_i64(&mut self, i: i64) {
        let mut tmp = itoa_buf();
        self.buf.extend_from_slice(fmt_i64(i, &mut tmp));
    }

    fn write_usize(&mut self, n: usize) {
        self.write_i64(n as i64);
    }
}

/// A stack scratch buffer large enough for any base-10 i64 (`-` + 19 digits).
fn itoa_buf() -> [u8; 20] {
    [0u8; 20]
}

/// Format `i` into `buf` and return the written slice. Avoids a heap
/// allocation on the reply hot path.
fn fmt_i64(i: i64, buf: &mut [u8; 20]) -> &[u8] {
    // Work with the magnitude as u64 to handle i64::MIN without overflow.
    let neg = i < 0;
    let mut mag = if neg {
        (i as i128).unsigned_abs() as u64
    } else {
        i as u64
    };
    let mut idx = buf.len();
    loop {
        idx -= 1;
        buf[idx] = b'0' + (mag % 10) as u8;
        mag /= 10;
        if mag == 0 {
            break;
        }
    }
    if neg {
        idx -= 1;
        buf[idx] = b'-';
    }
    &buf[idx..]
}

/// Redis-compatible double formatting.
fn fmt_double(f: f64) -> String {
    if f.is_nan() {
        "nan".to_string()
    } else if f.is_infinite() {
        if f > 0.0 {
            "inf".to_string()
        } else {
            "-inf".to_string()
        }
    } else {
        // Rust's default `{}` renders integral values without a trailing `.0`
        // (e.g. `3`, not `3.0`) and uses the shortest round-tripping form.
        format!("{}", f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- ReplyBuf goldens: RESP2 vs RESP3 ----

    fn r3() -> ReplyBuf {
        ReplyBuf::new(true)
    }
    fn r2() -> ReplyBuf {
        ReplyBuf::new(false)
    }

    #[test]
    fn simple_and_error() {
        let mut b = r3();
        b.simple("OK");
        assert_eq!(b.as_slice(), b"+OK\r\n");
        let mut b = r2();
        b.error("ERR bad thing");
        assert_eq!(b.as_slice(), b"-ERR bad thing\r\n");
    }

    #[test]
    fn integers() {
        let mut b = r3();
        b.int(0);
        b.int(42);
        b.int(-1);
        b.int(i64::MIN);
        b.int(i64::MAX);
        assert_eq!(
            b.take(),
            format!(":0\r\n:42\r\n:-1\r\n:{}\r\n:{}\r\n", i64::MIN, i64::MAX).into_bytes()
        );
    }

    #[test]
    fn bulk_strings() {
        let mut b = r3();
        b.bulk(b"hello");
        assert_eq!(b.as_slice(), b"$5\r\nhello\r\n");
        let mut b = r3();
        b.bulk(b"");
        assert_eq!(b.as_slice(), b"$0\r\n\r\n");
        let mut b = r3();
        b.bulk(b"a\r\nb");
        assert_eq!(b.as_slice(), b"$4\r\na\r\nb\r\n");
    }

    #[test]
    fn nulls() {
        let mut b = r3();
        b.null();
        assert_eq!(b.as_slice(), b"_\r\n");
        let mut b = r2();
        b.null();
        assert_eq!(b.as_slice(), b"$-1\r\n");

        let mut b = r3();
        b.null_array();
        assert_eq!(b.as_slice(), b"_\r\n");
        let mut b = r2();
        b.null_array();
        assert_eq!(b.as_slice(), b"*-1\r\n");
    }

    #[test]
    fn arrays_maps_sets_push() {
        let mut b = r3();
        b.array(3);
        assert_eq!(b.as_slice(), b"*3\r\n");

        let mut b = r3();
        b.map(2);
        assert_eq!(b.as_slice(), b"%2\r\n");
        let mut b = r2();
        b.map(2);
        assert_eq!(b.as_slice(), b"*4\r\n");

        let mut b = r3();
        b.set(3);
        assert_eq!(b.as_slice(), b"~3\r\n");
        let mut b = r2();
        b.set(3);
        assert_eq!(b.as_slice(), b"*3\r\n");

        let mut b = r3();
        b.push(2);
        assert_eq!(b.as_slice(), b">2\r\n");
        let mut b = r2();
        b.push(2);
        assert_eq!(b.as_slice(), b"*2\r\n");
    }

    #[test]
    fn booleans() {
        let mut b = r3();
        b.boolean(true);
        b.boolean(false);
        assert_eq!(b.as_slice(), b"#t\r\n#f\r\n");
        let mut b = r2();
        b.boolean(true);
        b.boolean(false);
        assert_eq!(b.as_slice(), b":1\r\n:0\r\n");
    }

    #[test]
    fn doubles_resp3() {
        let cases: &[(f64, &str)] = &[
            (3.0, ",3\r\n"),
            (3.5, ",3.5\r\n"),
            (-0.5, ",-0.5\r\n"),
            (0.0, ",0\r\n"),
            (f64::INFINITY, ",inf\r\n"),
            (f64::NEG_INFINITY, ",-inf\r\n"),
            (f64::NAN, ",nan\r\n"),
        ];
        for (f, want) in cases {
            let mut b = r3();
            b.double(*f);
            assert_eq!(b.as_slice(), want.as_bytes(), "double {f} resp3");
        }
    }

    #[test]
    fn doubles_resp2_are_bulk() {
        let mut b = r2();
        b.double(3.0);
        assert_eq!(b.as_slice(), b"$1\r\n3\r\n");
        let mut b = r2();
        b.double(3.25);
        assert_eq!(b.as_slice(), b"$4\r\n3.25\r\n");
        let mut b = r2();
        b.double(f64::INFINITY);
        assert_eq!(b.as_slice(), b"$3\r\ninf\r\n");
    }

    #[test]
    fn verbatim_strings() {
        let mut b = r3();
        b.verbatim("hello");
        assert_eq!(b.as_slice(), b"=9\r\ntxt:hello\r\n");
        let mut b = r2();
        b.verbatim("hello");
        assert_eq!(b.as_slice(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn take_and_len_and_empty() {
        let mut b = r3();
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);
        b.simple("OK");
        assert_eq!(b.len(), 5);
        assert!(!b.is_empty());
        let out = b.take();
        assert_eq!(out, b"+OK\r\n");
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);
    }

    // ---- parser unit tests (more in tests/parser.rs) ----

    #[test]
    fn basic_multibulk() {
        let mut p = RespParser::new();
        p.feed(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        let cmd = p.next_command().unwrap().unwrap();
        assert_eq!(cmd, vec![b"GET".to_vec(), b"foo".to_vec()]);
        assert_eq!(p.next_command().unwrap(), None);
        assert_eq!(p.buffered(), 0);
    }

    #[test]
    fn incomplete_returns_none() {
        let mut p = RespParser::new();
        p.feed(b"*2\r\n$3\r\nGET\r\n$3\r\nfo");
        assert_eq!(p.next_command().unwrap(), None);
        assert!(p.buffered() > 0);
    }

    #[test]
    fn empty_array_is_skipped() {
        let mut p = RespParser::new();
        p.feed(b"*0\r\n*1\r\n$4\r\nPING\r\n");
        let cmd = p.next_command().unwrap().unwrap();
        assert_eq!(cmd, vec![b"PING".to_vec()]);
    }

    #[test]
    fn oversized_bulk_rejected() {
        let mut p = RespParser::new();
        p.feed(format!("*1\r\n${}\r\n", MAX_BULK + 1).as_bytes());
        assert_eq!(p.next_command(), Err(RespError::TooLarge));
    }

    #[test]
    fn oversized_argc_rejected() {
        let mut p = RespParser::new();
        p.feed(format!("*{}\r\n", MAX_ARGS + 1).as_bytes());
        assert_eq!(p.next_command(), Err(RespError::TooLarge));
    }

    #[test]
    fn null_bulk_in_request_is_protocol_error() {
        let mut p = RespParser::new();
        p.feed(b"*1\r\n$-1\r\n");
        assert!(matches!(p.next_command(), Err(RespError::Protocol(_))));
    }

    #[test]
    fn negative_multibulk_is_protocol_error() {
        let mut p = RespParser::new();
        p.feed(b"*-1\r\n");
        assert!(matches!(p.next_command(), Err(RespError::Protocol(_))));
    }
}
