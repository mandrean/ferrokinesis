use ferrokinesis::event_stream::{
    encode_exception, encode_initial_response, encode_subscribe_event,
};

// -- encode_exception --

#[test]
fn encode_exception_produces_bytes() {
    let bytes = encode_exception("SomeException", "something went wrong");
    assert!(!bytes.is_empty());
    // AWS event stream frames start with a 4-byte prelude (total length)
    assert!(bytes.len() >= 4);
}

#[test]
fn encode_exception_different_types() {
    let a = encode_exception("ResourceNotFoundException", "stream not found");
    let b = encode_exception("ValidationException", "invalid argument");
    assert!(!a.is_empty());
    assert!(!b.is_empty());
    // Different exception types should produce different frames
    assert_ne!(a, b);
}

#[test]
fn encode_exception_empty_message() {
    let bytes = encode_exception("SomeException", "");
    assert!(!bytes.is_empty());
}

// -- encode_subscribe_event --

#[test]
fn encode_subscribe_event_produces_bytes() {
    let payload = br#"{"Records":[],"MillisBehindLatest":0}"#;
    let bytes = encode_subscribe_event(payload, "application/json");
    assert!(!bytes.is_empty());
    assert!(bytes.len() >= 4);
}

#[test]
fn encode_subscribe_event_empty_payload() {
    let bytes = encode_subscribe_event(b"{}", "application/json");
    assert!(!bytes.is_empty());
}

// -- encode_initial_response --

#[test]
fn encode_initial_response_produces_bytes() {
    let bytes = encode_initial_response();
    assert!(!bytes.is_empty());
    assert!(bytes.len() >= 4);
}
