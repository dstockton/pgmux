use anyhow::{bail, Result};
use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;

/// Maximum message size to prevent OOM (64MB).
const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// A raw Postgres wire protocol message.
#[derive(Debug, Clone)]
pub struct PgMessage {
    /// Message type byte (0 for startup messages).
    pub msg_type: u8,
    /// Full payload (excluding the type byte and length).
    pub payload: BytesMut,
}

impl PgMessage {
    /// Encode the message into a buffer ready for sending.
    pub fn encode(&self, buf: &mut BytesMut) {
        if self.msg_type != 0 {
            buf.put_u8(self.msg_type);
        }
        // Length includes itself (4 bytes) + payload
        buf.put_i32((self.payload.len() + 4) as i32);
        buf.extend_from_slice(&self.payload);
    }

    /// Create a message from type and payload.
    pub fn new(msg_type: u8, payload: BytesMut) -> Self {
        Self { msg_type, payload }
    }
}

/// Read a single Postgres message from a buffer.
/// Returns None if the buffer doesn't have enough data yet.
pub fn try_read_message(buf: &mut BytesMut, is_startup: bool) -> Result<Option<PgMessage>> {
    if is_startup {
        // Startup messages: 4-byte length, then payload (no type byte)
        if buf.len() < 4 {
            return Ok(None);
        }
        let len = (&buf[0..4]).get_i32() as usize;
        if !(4..=MAX_MESSAGE_SIZE).contains(&len) {
            bail!("Invalid startup message length: {}", len);
        }
        if buf.len() < len {
            return Ok(None);
        }
        buf.advance(4); // skip length
        let payload = buf.split_to(len - 4);
        Ok(Some(PgMessage {
            msg_type: 0,
            payload,
        }))
    } else {
        // Normal messages: 1-byte type, 4-byte length, then payload
        if buf.len() < 5 {
            return Ok(None);
        }
        let msg_type = buf[0];
        let len = (&buf[1..5]).get_i32() as usize;
        if !(4..=MAX_MESSAGE_SIZE).contains(&len) {
            bail!(
                "Invalid message length: {} for type '{}'",
                len,
                msg_type as char
            );
        }
        let total_len = 1 + len;
        if buf.len() < total_len {
            return Ok(None);
        }
        buf.advance(5); // skip type + length
        let payload = buf.split_to(len - 4);
        Ok(Some(PgMessage { msg_type, payload }))
    }
}

/// Parse startup message parameters into a HashMap.
pub fn parse_startup_params(payload: &[u8]) -> Result<(i32, HashMap<String, String>)> {
    if payload.len() < 4 {
        bail!("Startup payload too short");
    }

    let protocol_version = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let mut params = HashMap::new();
    let mut pos = 4;

    // Read null-terminated key-value pairs
    while pos < payload.len() {
        if payload[pos] == 0 {
            break;
        }
        let key_start = pos;
        while pos < payload.len() && payload[pos] != 0 {
            pos += 1;
        }
        let key = std::str::from_utf8(&payload[key_start..pos])?.to_string();
        pos += 1; // skip null

        let val_start = pos;
        while pos < payload.len() && payload[pos] != 0 {
            pos += 1;
        }
        let val = std::str::from_utf8(&payload[val_start..pos])?.to_string();
        pos += 1; // skip null

        params.insert(key, val);
    }

    Ok((protocol_version, params))
}

/// Build a startup message for connecting to the backend.
pub fn build_startup_message(
    user: &str,
    database: &str,
    extra_params: &[(String, String)],
) -> BytesMut {
    let mut payload = BytesMut::new();
    // Protocol version 3.0
    payload.put_i32(196608); // 3 << 16

    // user
    payload.extend_from_slice(b"user\0");
    payload.extend_from_slice(user.as_bytes());
    payload.put_u8(0);

    // database
    payload.extend_from_slice(b"database\0");
    payload.extend_from_slice(database.as_bytes());
    payload.put_u8(0);

    // extra params
    for (k, v) in extra_params {
        payload.extend_from_slice(k.as_bytes());
        payload.put_u8(0);
        payload.extend_from_slice(v.as_bytes());
        payload.put_u8(0);
    }

    // terminator
    payload.put_u8(0);

    // Build the full message: length + payload
    let mut msg = BytesMut::new();
    msg.put_i32((payload.len() + 4) as i32);
    msg.extend_from_slice(&payload);
    msg
}

/// Build an ErrorResponse message.
pub fn build_error_response(severity: &str, code: &str, message: &str) -> BytesMut {
    let mut payload = BytesMut::new();
    // Severity
    payload.put_u8(b'S');
    payload.extend_from_slice(severity.as_bytes());
    payload.put_u8(0);
    // Severity (non-localized)
    payload.put_u8(b'V');
    payload.extend_from_slice(severity.as_bytes());
    payload.put_u8(0);
    // SQLSTATE code
    payload.put_u8(b'C');
    payload.extend_from_slice(code.as_bytes());
    payload.put_u8(0);
    // Message
    payload.put_u8(b'M');
    payload.extend_from_slice(message.as_bytes());
    payload.put_u8(0);
    // Terminator
    payload.put_u8(0);

    let mut buf = BytesMut::new();
    buf.put_u8(b'E');
    buf.put_i32((payload.len() + 4) as i32);
    buf.extend_from_slice(&payload);
    buf
}

/// Build a NoticeResponse message.
pub fn build_notice_response(severity: &str, code: &str, message: &str) -> BytesMut {
    let mut payload = BytesMut::new();
    payload.put_u8(b'S');
    payload.extend_from_slice(severity.as_bytes());
    payload.put_u8(0);
    payload.put_u8(b'V');
    payload.extend_from_slice(severity.as_bytes());
    payload.put_u8(0);
    payload.put_u8(b'C');
    payload.extend_from_slice(code.as_bytes());
    payload.put_u8(0);
    payload.put_u8(b'M');
    payload.extend_from_slice(message.as_bytes());
    payload.put_u8(0);
    payload.put_u8(0);

    let mut buf = BytesMut::new();
    buf.put_u8(b'N');
    buf.put_i32((payload.len() + 4) as i32);
    buf.extend_from_slice(&payload);
    buf
}

/// Build an AuthenticationOk message.
pub fn build_auth_ok() -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_u8(b'R');
    buf.put_i32(8);
    buf.put_i32(0);
    buf
}

/// Build an AuthenticationCleartextPassword request.
pub fn build_auth_cleartext_request() -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_u8(b'R');
    buf.put_i32(8);
    buf.put_i32(3);
    buf
}

/// Build a ReadyForQuery message.
pub fn build_ready_for_query(status: u8) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_u8(b'Z');
    buf.put_i32(5);
    buf.put_u8(status);
    buf
}

/// Build ParameterStatus message.
pub fn build_parameter_status(name: &str, value: &str) -> BytesMut {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(name.as_bytes());
    payload.put_u8(0);
    payload.extend_from_slice(value.as_bytes());
    payload.put_u8(0);

    let mut buf = BytesMut::new();
    buf.put_u8(b'S');
    buf.put_i32((payload.len() + 4) as i32);
    buf.extend_from_slice(&payload);
    buf
}

/// Build a BackendKeyData message.
pub fn build_backend_key_data(pid: i32, secret: i32) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_u8(b'K');
    buf.put_i32(12);
    buf.put_i32(pid);
    buf.put_i32(secret);
    buf
}

/// Check if a query message contains a statement that could shrink the database
/// (DELETE, TRUNCATE, DROP, VACUUM).
pub fn is_shrink_operation(query: &str) -> bool {
    let upper = query.trim().to_uppercase();
    upper.starts_with("DELETE")
        || upper.starts_with("TRUNCATE")
        || upper.starts_with("DROP")
        || upper.starts_with("VACUUM")
}

/// Check if a query is a simple query message and extract the SQL.
pub fn extract_query_text(msg: &PgMessage) -> Option<String> {
    if msg.msg_type == b'Q' {
        // Simple query: null-terminated string
        let payload = &msg.payload;
        if let Some(null_pos) = payload.iter().position(|&b| b == 0) {
            return std::str::from_utf8(&payload[..null_pos])
                .ok()
                .map(|s| s.to_string());
        }
    }
    None
}

/// SSL request magic number.
pub const SSL_REQUEST_CODE: i32 = 80877103;

/// Cancel request magic number.
pub const CANCEL_REQUEST_CODE: i32 = 80877102;

/// Lightweight message-boundary scanner for the fast proxy path.
/// Tracks position within the PG wire protocol stream without full message parsing.
/// Only inspects ReadyForQuery messages for transaction state.
pub struct MessageBoundaryScanner {
    /// Bytes remaining to skip in the current message payload.
    pub(crate) remaining_skip: usize,
    /// Partial header bytes carried from previous read.
    header_buf: [u8; 5],
    /// How many header bytes we have buffered.
    header_len: usize,
    /// Last observed transaction status from ReadyForQuery (b'I', b'T', b'E').
    pub transaction_status: u8,
}

impl MessageBoundaryScanner {
    pub fn new() -> Self {
        Self {
            remaining_skip: 0,
            header_buf: [0u8; 5],
            header_len: 0,
            transaction_status: b'I',
        }
    }

    /// Scan a buffer of bytes from the server->client direction.
    /// Updates transaction_status when ReadyForQuery messages are found.
    /// Returns true if the most recent ReadyForQuery had status 'I' (idle).
    pub fn scan(&mut self, data: &[u8]) -> bool {
        let mut pos = 0;
        let mut saw_idle = false;

        while pos < data.len() {
            // Phase 1: skip remaining payload bytes from previous message
            if self.remaining_skip > 0 {
                let skip = std::cmp::min(self.remaining_skip, data.len() - pos);
                pos += skip;
                self.remaining_skip -= skip;
                continue;
            }

            // Phase 2: accumulate header bytes (type + 4-byte length = 5 bytes)
            if self.header_len < 5 {
                let need = 5 - self.header_len;
                let avail = std::cmp::min(need, data.len() - pos);
                self.header_buf[self.header_len..self.header_len + avail]
                    .copy_from_slice(&data[pos..pos + avail]);
                self.header_len += avail;
                pos += avail;

                if self.header_len < 5 {
                    break; // need more data
                }
            }

            // Phase 3: we have a full 5-byte header
            let msg_type = self.header_buf[0];
            let len = i32::from_be_bytes([
                self.header_buf[1],
                self.header_buf[2],
                self.header_buf[3],
                self.header_buf[4],
            ]) as usize;

            let payload_len = if len >= 4 { len - 4 } else { 0 };

            if msg_type == b'Z' && payload_len == 1 {
                // ReadyForQuery — need 1 byte of payload for transaction status
                if pos < data.len() {
                    self.transaction_status = data[pos];
                    pos += 1;
                    self.remaining_skip = 0;
                    saw_idle = self.transaction_status == b'I';
                } else {
                    self.remaining_skip = 1;
                }
            } else {
                self.remaining_skip = payload_len;
            }

            self.header_len = 0; // reset for next message
        }

        saw_idle
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_and_parse_startup() {
        let msg = build_startup_message("testuser", "testdb", &[]);
        // Parse it back
        let mut buf = msg.clone();
        let len = (&buf[0..4]).get_i32() as usize;
        buf.advance(4);
        let payload = &buf[..len - 4];
        let (version, params) = parse_startup_params(payload).unwrap();
        assert_eq!(version, 196608);
        assert_eq!(params.get("user").unwrap(), "testuser");
        assert_eq!(params.get("database").unwrap(), "testdb");
    }

    #[test]
    fn test_error_response() {
        let buf = build_error_response("ERROR", "28000", "auth failed");
        assert_eq!(buf[0], b'E');
    }

    #[test]
    fn test_is_shrink_operation() {
        assert!(is_shrink_operation("DELETE FROM foo"));
        assert!(is_shrink_operation("TRUNCATE TABLE bar"));
        assert!(is_shrink_operation("  DROP TABLE baz"));
        assert!(is_shrink_operation("VACUUM FULL"));
        assert!(!is_shrink_operation("SELECT 1"));
        assert!(!is_shrink_operation("INSERT INTO foo VALUES (1)"));
    }

    #[test]
    fn test_try_read_message() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        let query = b"SELECT 1\0";
        buf.put_i32((query.len() + 4) as i32);
        buf.extend_from_slice(query);

        let msg = try_read_message(&mut buf, false).unwrap().unwrap();
        assert_eq!(msg.msg_type, b'Q');
        let q = extract_query_text(&msg).unwrap();
        assert_eq!(q, "SELECT 1");
    }

    #[test]
    fn test_scanner_single_ready_for_query() {
        let mut scanner = MessageBoundaryScanner::new();
        let data = [0x5A, 0x00, 0x00, 0x00, 0x05, b'I'];
        assert!(scanner.scan(&data));
        assert_eq!(scanner.transaction_status, b'I');
    }

    #[test]
    fn test_scanner_transaction_state_tracking() {
        let mut scanner = MessageBoundaryScanner::new();
        let data = [0x5A, 0x00, 0x00, 0x00, 0x05, b'T'];
        assert!(!scanner.scan(&data));
        assert_eq!(scanner.transaction_status, b'T');

        let data = [0x5A, 0x00, 0x00, 0x00, 0x05, b'I'];
        assert!(scanner.scan(&data));
        assert_eq!(scanner.transaction_status, b'I');
    }

    #[test]
    fn test_scanner_skips_non_z_messages() {
        let mut scanner = MessageBoundaryScanner::new();
        let mut data = Vec::new();
        data.push(b'D');
        data.extend_from_slice(&8i32.to_be_bytes());
        data.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]);
        data.extend_from_slice(&[0x5A, 0x00, 0x00, 0x00, 0x05, b'I']);
        assert!(scanner.scan(&data));
        assert_eq!(scanner.transaction_status, b'I');
    }

    #[test]
    fn test_scanner_split_across_reads() {
        let mut scanner = MessageBoundaryScanner::new();
        let part1 = [0x5A, 0x00, 0x00];
        let part2 = [0x00, 0x05, b'I'];
        assert!(!scanner.scan(&part1));
        assert!(scanner.scan(&part2));
        assert_eq!(scanner.transaction_status, b'I');
    }

    #[test]
    fn test_scanner_large_message_then_rfq() {
        let mut scanner = MessageBoundaryScanner::new();
        let mut data = Vec::new();
        data.push(b'T');
        data.extend_from_slice(&104i32.to_be_bytes());
        data.extend_from_slice(&vec![0u8; 100]);
        data.extend_from_slice(&[0x5A, 0x00, 0x00, 0x00, 0x05, b'I']);
        assert!(scanner.scan(&data));
        assert_eq!(scanner.transaction_status, b'I');
    }

    #[test]
    fn test_scanner_payload_split_across_reads() {
        let mut scanner = MessageBoundaryScanner::new();
        let mut part1 = Vec::new();
        part1.push(b'D');
        part1.extend_from_slice(&24i32.to_be_bytes());
        part1.extend_from_slice(&vec![0xAA; 10]);
        assert!(!scanner.scan(&part1));
        assert_eq!(scanner.remaining_skip, 10);

        let mut part2 = Vec::new();
        part2.extend_from_slice(&vec![0xBB; 10]);
        part2.extend_from_slice(&[0x5A, 0x00, 0x00, 0x00, 0x05, b'I']);
        assert!(scanner.scan(&part2));
        assert_eq!(scanner.transaction_status, b'I');
    }
}
