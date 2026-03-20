use aws_smithy_eventstream::frame::write_message_to;
use aws_smithy_types::event_stream::{Header, HeaderValue, Message};
use bytes::Bytes;

/// Encode a SubscribeToShardEvent as an AWS event stream binary frame.
pub fn encode_subscribe_event(payload: &[u8], content_type: &str) -> Vec<u8> {
    let message = Message::new(Bytes::copy_from_slice(payload))
        .add_header(Header::new(
            ":message-type",
            HeaderValue::String("event".into()),
        ))
        .add_header(Header::new(
            ":event-type",
            HeaderValue::String("SubscribeToShardEvent".into()),
        ))
        .add_header(Header::new(
            ":content-type",
            HeaderValue::String(content_type.to_string().into()),
        ));

    let mut buf = Vec::new();
    write_message_to(&message, &mut buf).expect("failed to encode event stream message");
    buf
}

/// Encode an initial-response event stream frame (HTTP 200 confirmation).
pub fn encode_initial_response() -> Vec<u8> {
    let message = Message::new(Bytes::new())
        .add_header(Header::new(
            ":message-type",
            HeaderValue::String("event".into()),
        ))
        .add_header(Header::new(
            ":event-type",
            HeaderValue::String("initial-response".into()),
        ));

    let mut buf = Vec::new();
    write_message_to(&message, &mut buf).expect("failed to encode event stream message");
    buf
}

/// Encode an exception as an AWS event stream binary frame.
pub fn encode_exception(exception_type: &str, message_text: &str) -> Vec<u8> {
    let payload = serde_json::json!({"message": message_text}).to_string();
    let message = Message::new(Bytes::from(payload))
        .add_header(Header::new(
            ":message-type",
            HeaderValue::String("exception".into()),
        ))
        .add_header(Header::new(
            ":exception-type",
            HeaderValue::String(exception_type.to_string().into()),
        ))
        .add_header(Header::new(
            ":content-type",
            HeaderValue::String("application/json".into()),
        ));

    let mut buf = Vec::new();
    write_message_to(&message, &mut buf).expect("failed to encode event stream message");
    buf
}
