use bytes::{Buf, BufMut};

pub fn put_varint(buf: &mut Vec<u8>, value: i8) {
    let mut x = ((value << 1) ^ (value >> 7)) as u8;
    while x & 0x80 != 0 {
        buf.put_u8((x & 0x7F) | 0x80);
        x >>= 7;
    }
    buf.put_u8(x);
}

pub fn append_msg_len(buf: &mut [u8]) -> Vec<u8> {
    let mut final_response = Vec::new();
    final_response.put_i32(buf.len() as i32);
    final_response.extend(buf.iter());
    final_response
}

pub fn read_request_header_v0(buf: &mut &[u8]) -> (i16, i16, i32) {
    let api_key = buf.get_i16();
    let api_version = buf.get_i16();
    let correlation_id = buf.get_i32();

    (api_key, api_version, correlation_id)
}

pub fn read_request_header_v1(buf: &mut &[u8]) -> (i16, i16, i32, Option<Vec<u8>>) {
    let api_key = buf.get_i16();
    let api_version = buf.get_i16();
    let correlation_id = buf.get_i32();

    let client_id_len = buf.get_i16();
    let client_id = if client_id_len == -1 { // Client ID = Nullable String
        None
    } else {
        let mut client_id = vec![0; client_id_len as usize];
        buf.copy_to_slice(&mut client_id);
        Some(client_id)
    };

    (api_key, api_version, correlation_id, client_id)
}

pub fn read_request_header_v2(buf: &mut &[u8]) -> (i16, i16, i32, Option<Vec<u8>>) {
    let api_key = buf.get_i16();
    let api_version = buf.get_i16();
    let correlation_id = buf.get_i32();

    let client_id_len = buf.get_i16(); // Client ID = Nullable String
    let client_id = if client_id_len == -1 {
        None
    } else {
        let mut client_id = vec![0; client_id_len as usize];
        buf.copy_to_slice(&mut client_id);
        Some(client_id)
    };

    buf.advance(1); // TAG_BUFFER

    (api_key, api_version, correlation_id, client_id)
}

pub fn write_resp_header_v0(buf: &mut Vec<u8>, correlation_id: i32) {
    buf.put_i32(correlation_id);
}

pub fn write_resp_header_v1(buf: &mut Vec<u8>, correlation_id: i32) {
    buf.put_i32(correlation_id);
    buf.put_i8(0); // TAG_BUFFER
}
