//! Integration tests for the RESP request parser, driven only through the
//! public [`RespParser`] API.

use marekvs_resp::{RespError, RespParser, MAX_ARGS, MAX_BULK};

fn args(v: &[&[u8]]) -> Vec<Vec<u8>> {
    v.iter().map(|s| s.to_vec()).collect()
}

#[test]
fn pipelined_commands_in_one_feed() {
    let mut p = RespParser::new();
    p.feed(b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nx\r\n*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\ny\r\n");

    assert_eq!(p.next_command().unwrap().unwrap(), args(&[b"PING"]));
    assert_eq!(p.next_command().unwrap().unwrap(), args(&[b"GET", b"x"]));
    assert_eq!(
        p.next_command().unwrap().unwrap(),
        args(&[b"SET", b"x", b"y"])
    );
    assert_eq!(p.next_command().unwrap(), None);
    assert_eq!(p.buffered(), 0);
}

#[test]
fn byte_at_a_time_multibulk() {
    let full = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
    let mut p = RespParser::new();
    // Feed one byte at a time; only the final byte should complete the command.
    for (i, &byte) in full.iter().enumerate() {
        p.feed(&[byte]);
        let res = p.next_command().unwrap();
        if i + 1 == full.len() {
            assert_eq!(res.unwrap(), args(&[b"hello", b"world"]));
        } else {
            assert_eq!(res, None, "unexpected command after {} bytes", i + 1);
        }
    }
    assert_eq!(p.buffered(), 0);
}

#[test]
fn chunked_feed_across_boundaries() {
    let mut p = RespParser::new();
    p.feed(b"*2\r\n$3\r\nGE");
    assert_eq!(p.next_command().unwrap(), None);
    p.feed(b"T\r\n$3\r\nf");
    assert_eq!(p.next_command().unwrap(), None);
    p.feed(b"oo\r\n");
    assert_eq!(p.next_command().unwrap().unwrap(), args(&[b"GET", b"foo"]));
}

#[test]
fn inline_command_crlf() {
    let mut p = RespParser::new();
    p.feed(b"PING\r\n");
    assert_eq!(p.next_command().unwrap().unwrap(), args(&[b"PING"]));
}

#[test]
fn inline_command_lf_only() {
    let mut p = RespParser::new();
    p.feed(b"PING\n");
    assert_eq!(p.next_command().unwrap().unwrap(), args(&[b"PING"]));
}

#[test]
fn inline_multiple_args_and_whitespace() {
    let mut p = RespParser::new();
    p.feed(b"   SET   foo   bar  \r\n");
    assert_eq!(
        p.next_command().unwrap().unwrap(),
        args(&[b"SET", b"foo", b"bar"])
    );
}

#[test]
fn empty_inline_line_is_skipped() {
    let mut p = RespParser::new();
    p.feed(b"\r\n   \r\nPING\r\n");
    assert_eq!(p.next_command().unwrap().unwrap(), args(&[b"PING"]));
    assert_eq!(p.next_command().unwrap(), None);
}

#[test]
fn inline_mixed_with_typed() {
    let mut p = RespParser::new();
    p.feed(b"PING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\nECHO hi\n");
    assert_eq!(p.next_command().unwrap().unwrap(), args(&[b"PING"]));
    assert_eq!(p.next_command().unwrap().unwrap(), args(&[b"GET", b"k"]));
    assert_eq!(p.next_command().unwrap().unwrap(), args(&[b"ECHO", b"hi"]));
    assert_eq!(p.next_command().unwrap(), None);
}

#[test]
fn binary_safe_bulk_payload() {
    // Bulk strings must be length-prefixed and binary safe (embedded CRLF, NUL).
    let mut p = RespParser::new();
    // 5-byte payload containing CRLF and a NUL byte, then the framing CRLF.
    p.feed(b"*1\r\n$5\r\na\r\nb\x00\r\n");
    assert_eq!(p.next_command().unwrap().unwrap(), args(&[b"a\r\nb\x00"]));
}

#[test]
fn oversized_bulk_is_too_large() {
    let mut p = RespParser::new();
    p.feed(format!("*1\r\n${}\r\n", MAX_BULK + 1).as_bytes());
    assert_eq!(p.next_command(), Err(RespError::TooLarge));
}

#[test]
fn max_bulk_boundary_is_accepted_when_data_present() {
    // A bulk of exactly MAX_BULK is allowed by the limit check (we only feed the
    // header here, so it stays incomplete rather than erroring).
    let mut p = RespParser::new();
    p.feed(format!("*1\r\n${}\r\n", MAX_BULK).as_bytes());
    assert_eq!(p.next_command().unwrap(), None);
}

#[test]
fn oversized_argc_is_too_large() {
    let mut p = RespParser::new();
    p.feed(format!("*{}\r\n", MAX_ARGS + 1).as_bytes());
    assert_eq!(p.next_command(), Err(RespError::TooLarge));
}

#[test]
fn negative_bulk_length_is_protocol_error() {
    let mut p = RespParser::new();
    p.feed(b"*1\r\n$-1\r\n");
    assert!(matches!(p.next_command(), Err(RespError::Protocol(_))));
}

#[test]
fn negative_multibulk_length_is_protocol_error() {
    let mut p = RespParser::new();
    p.feed(b"*-1\r\n");
    assert!(matches!(p.next_command(), Err(RespError::Protocol(_))));
}

#[test]
fn non_dollar_element_is_protocol_error() {
    let mut p = RespParser::new();
    p.feed(b"*1\r\n:5\r\n");
    assert!(matches!(p.next_command(), Err(RespError::Protocol(_))));
}

#[test]
fn missing_crlf_after_bulk_is_protocol_error() {
    let mut p = RespParser::new();
    // Payload "foo" is followed by "XX" instead of CRLF.
    p.feed(b"*1\r\n$3\r\nfooXX");
    assert!(matches!(p.next_command(), Err(RespError::Protocol(_))));
}

#[test]
fn bare_lf_in_typed_frame_is_protocol_error() {
    let mut p = RespParser::new();
    // Multibulk count line terminated by LF only (strict CRLF required).
    p.feed(b"*1\n");
    assert!(matches!(p.next_command(), Err(RespError::Protocol(_))));
}

#[test]
fn non_numeric_length_is_protocol_error() {
    let mut p = RespParser::new();
    p.feed(b"*abc\r\n");
    assert!(matches!(p.next_command(), Err(RespError::Protocol(_))));
}

#[test]
fn buffered_tracks_unconsumed_bytes() {
    let mut p = RespParser::new();
    p.feed(b"*1\r\n$4\r\nPING\r\n*1\r\n$2\r\nhi");
    assert_eq!(p.next_command().unwrap().unwrap(), args(&[b"PING"]));
    // The second, incomplete command's bytes remain buffered.
    assert_eq!(p.next_command().unwrap(), None);
    assert_eq!(p.buffered(), "*1\r\n$2\r\nhi".len());
}

#[test]
fn many_commands_compaction_stays_correct() {
    // Push far more than the compaction threshold through the parser to exercise
    // the buffer drain path and confirm no bytes are lost or duplicated.
    let mut p = RespParser::new();
    let mut expected = 0usize;
    for i in 0..5000u32 {
        let val = i.to_string();
        p.feed(format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", val.len(), val).as_bytes());
        expected += 1;
    }
    let mut seen = 0usize;
    while let Some(cmd) = p.next_command().unwrap() {
        assert_eq!(cmd[0], b"GET");
        assert_eq!(cmd[1], seen.to_string().into_bytes());
        seen += 1;
    }
    assert_eq!(seen, expected);
    assert_eq!(p.buffered(), 0);
}
