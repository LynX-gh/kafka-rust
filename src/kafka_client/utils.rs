use bytes::{Buf, BufMut};

pub fn append_msg_len(buf: &mut Vec<u8>) {
    todo!("Complete Utils and Refactor Code");
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
    let client_id = if client_id_len == -1 {
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

    let client_id_len = buf.get_i16();
    let client_id = if client_id_len == -1 {
        None
    } else {
        let mut client_id = vec![0; client_id_len as usize];
        buf.copy_to_slice(&mut client_id);
        Some(client_id)
    };

    buf.advance(1);
    (api_key, api_version, correlation_id, client_id)
}

pub fn write_resp_header_v0(buf: &mut Vec<u8>) {
    todo!("Complete Utils and Refactor Code");
}

pub fn write_resp_header_v1(buf: &mut Vec<u8>) {
    todo!("Complete Utils and Refactor Code");
}
