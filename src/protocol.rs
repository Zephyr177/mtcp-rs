use std::io;

pub const HEADER_LEN: usize = 4;
pub const MAX_FRAME_PAYLOAD: usize = 65_536;

pub fn next_pid(pid: u16) -> u16 {
    pid.wrapping_add(1)
}

pub fn encode_login(cid: u16, mid: u16) -> [u8; 4] {
    let mut bytes = [0u8; 4];
    bytes[..2].copy_from_slice(&(cid + 100).to_be_bytes());
    bytes[2..].copy_from_slice(&mid.to_be_bytes());
    bytes
}

pub fn parse_login(bytes: [u8; 4], expected_cid: u16) -> io::Result<u16> {
    let login = u16::from_be_bytes([bytes[0], bytes[1]]);
    if login != expected_cid + 100 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid mtcp login header",
        ));
    }
    Ok(u16::from_be_bytes([bytes[2], bytes[3]]))
}

pub fn encode_frame(pid: u16, payload: &[u8]) -> Vec<u8> {
    assert!(!payload.is_empty(), "mtcp payload must not be empty");
    assert!(
        payload.len() <= MAX_FRAME_PAYLOAD,
        "mtcp payload exceeds frame limit"
    );

    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
    frame.extend_from_slice(&((payload.len() - 1) as u16).to_be_bytes());
    frame.extend_from_slice(&pid.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

pub fn try_parse_frame(buffer: &[u8]) -> Option<(usize, u16, usize)> {
    if buffer.len() < HEADER_LEN {
        return None;
    }

    let payload_len = u16::from_be_bytes([buffer[0], buffer[1]]) as usize + 1;
    let frame_len = HEADER_LEN + payload_len;
    if buffer.len() < frame_len {
        return None;
    }

    let pid = u16::from_be_bytes([buffer[2], buffer[3]]);
    Some((frame_len, pid, payload_len))
}

#[cfg(test)]
mod tests {
    use super::{encode_frame, encode_login, parse_login, try_parse_frame, HEADER_LEN};

    #[test]
    fn login_roundtrip_matches_node_protocol() {
        let bytes = encode_login(123, 456);
        assert_eq!(parse_login(bytes, 123).unwrap(), 456);
    }

    #[test]
    fn frame_roundtrip_matches_node_protocol() {
        let payload = b"hello world";
        let frame = encode_frame(17, payload);
        let (frame_len, pid, payload_len) = try_parse_frame(&frame).unwrap();

        assert_eq!(frame_len, HEADER_LEN + payload.len());
        assert_eq!(pid, 17);
        assert_eq!(payload_len, payload.len());
        assert_eq!(&frame[HEADER_LEN..], payload);
    }
}
