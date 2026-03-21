use axum::body::{Body, to_bytes};
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderName, HeaderValue, Request, StatusCode, Version};
use ferrokinesis::store::StoreOptions;
use std::env;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::ExitCode;
use std::str::FromStr;
use std::time::Duration;
use tower::util::ServiceExt;

const DEFAULT_PORT: u16 = 4567;
const DEFAULT_MAX_REQUEST_BODY_MB: u64 = 7;
const DEFAULT_LOG_LEVEL: &str = "info";
const HEADER_LIMIT_BYTES: usize = 64 * 1024;
const IDLE_TICK_MS: u64 = 10;
const SOCKET_TIMEOUT_SECS: u64 = 5;

type DynError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Debug)]
struct WasiConfig {
    port: u16,
    max_request_body_mb: u64,
    log_level: String,
    store_options: StoreOptions,
}

#[derive(Debug, PartialEq, Eq)]
struct ParsedRequest {
    method: String,
    path: String,
    version: u8,
    headers: Vec<(String, Vec<u8>)>,
    body: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
struct ParsedHead {
    request: ParsedRequest,
    body_target_len: usize,
}

struct SerializedResponse {
    status: StatusCode,
    headers: Vec<(String, Vec<u8>)>,
    body: Vec<u8>,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), DynError> {
    let config = WasiConfig::from_env()?;
    init_tracing(&config.log_level)?;

    let listener = TcpListener::bind(("0.0.0.0", config.port))?;
    listener.set_nonblocking(true)?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()?;
    let _enter = runtime.enter();
    let (app, _store_guard) = ferrokinesis::create_app(config.store_options.clone());
    let app = app.layer(DefaultBodyLimit::max(config.max_request_body_bytes()));
    drop(_enter);

    tracing::info!(
        address = %format!("0.0.0.0:{}", config.port),
        "Listening via WASI TCP adapter"
    );

    loop {
        match listener.accept() {
            Ok((mut stream, peer)) => {
                stream.set_read_timeout(Some(Duration::from_secs(SOCKET_TIMEOUT_SECS)))?;
                stream.set_write_timeout(Some(Duration::from_secs(SOCKET_TIMEOUT_SECS)))?;
                if let Err(err) = serve_connection(
                    &runtime,
                    app.clone(),
                    &mut stream,
                    config.max_request_body_bytes(),
                ) {
                    tracing::warn!(%peer, error = %err, "request handling failed");
                }
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                runtime.block_on(tokio::time::sleep(Duration::from_millis(IDLE_TICK_MS)));
            }
            Err(err) => return Err(Box::new(err)),
        }
    }
}

impl WasiConfig {
    fn from_env() -> io::Result<Self> {
        let defaults = StoreOptions::default();
        let port = read_env("FERROKINESIS_PORT")?.unwrap_or(DEFAULT_PORT);
        let max_request_body_mb =
            read_env("FERROKINESIS_MAX_REQUEST_BODY_MB")?.unwrap_or(DEFAULT_MAX_REQUEST_BODY_MB);
        if max_request_body_mb == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "FERROKINESIS_MAX_REQUEST_BODY_MB must be greater than zero",
            ));
        }

        let aws_region = read_env("AWS_REGION")?
            .or(read_env("AWS_DEFAULT_REGION")?)
            .unwrap_or_else(|| defaults.aws_region.clone());

        Ok(Self {
            port,
            max_request_body_mb,
            log_level: read_env("FERROKINESIS_LOG_LEVEL")?
                .unwrap_or_else(|| DEFAULT_LOG_LEVEL.to_string()),
            store_options: StoreOptions {
                create_stream_ms: read_env("FERROKINESIS_CREATE_STREAM_MS")?
                    .unwrap_or(defaults.create_stream_ms),
                delete_stream_ms: read_env("FERROKINESIS_DELETE_STREAM_MS")?
                    .unwrap_or(defaults.delete_stream_ms),
                update_stream_ms: read_env("FERROKINESIS_UPDATE_STREAM_MS")?
                    .unwrap_or(defaults.update_stream_ms),
                shard_limit: read_env("FERROKINESIS_SHARD_LIMIT")?.unwrap_or(defaults.shard_limit),
                iterator_ttl_seconds: read_env("FERROKINESIS_ITERATOR_TTL_SECONDS")?
                    .unwrap_or(defaults.iterator_ttl_seconds),
                retention_check_interval_secs: read_env(
                    "FERROKINESIS_RETENTION_CHECK_INTERVAL_SECS",
                )?
                .unwrap_or(defaults.retention_check_interval_secs),
                aws_account_id: read_env("AWS_ACCOUNT_ID")?
                    .unwrap_or_else(|| defaults.aws_account_id.clone()),
                aws_region,
            },
        })
    }

    fn max_request_body_bytes(&self) -> usize {
        self.max_request_body_mb
            .saturating_mul(1024 * 1024)
            .try_into()
            .expect("max request body size overflows usize")
    }
}

fn init_tracing(log_level: &str) -> Result<(), DynError> {
    let env_filter = if env::var("RUST_LOG").is_ok_and(|value| !value.is_empty()) {
        tracing_subscriber::EnvFilter::from_default_env()
    } else {
        tracing_subscriber::EnvFilter::new(log_level)
    };

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .try_init()?;
    Ok(())
}

fn serve_connection(
    runtime: &tokio::runtime::Runtime,
    app: axum::Router,
    stream: &mut TcpStream,
    max_body_bytes: usize,
) -> io::Result<()> {
    let request = match read_http_request(stream, max_body_bytes)? {
        Some(request) => request,
        None => return Ok(()),
    };

    let response = match runtime.block_on(execute_request(app, request)) {
        Ok(response) => response,
        Err(err) => {
            write_plain_response(
                stream,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal Server Error",
            )?;
            return Err(err);
        }
    };

    write_http_response(stream, response)
}

fn read_http_request(
    stream: &mut TcpStream,
    max_body_bytes: usize,
) -> io::Result<Option<ParsedRequest>> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 8192];

    loop {
        match stream.read(&mut chunk) {
            Ok(0) => {
                if buffer.is_empty() {
                    return Ok(None);
                }
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed before request headers were complete",
                ));
            }
            Ok(read) => {
                buffer.extend_from_slice(&chunk[..read]);
                if header_end(&buffer).is_some() {
                    break;
                }
                if buffer.len() > HEADER_LIMIT_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "request headers exceeded limit",
                    ));
                }
            }
            Err(err) => return Err(err),
        }
    }

    let ParsedHead {
        mut request,
        body_target_len,
    } = parse_request_head(&buffer, max_body_bytes)?;
    let body_start = header_end(&buffer).expect("header end checked above");

    let already_buffered = &buffer[body_start..];
    request
        .body
        .extend_from_slice(&already_buffered[..already_buffered.len().min(body_target_len)]);

    while request.body.len() < body_target_len {
        let to_read = (body_target_len - request.body.len()).min(chunk.len());
        let read = stream.read(&mut chunk[..to_read])?;
        if read == 0 {
            break;
        }
        request.body.extend_from_slice(&chunk[..read]);
    }

    Ok(Some(request))
}

fn parse_request_head(buffer: &[u8], max_body_bytes: usize) -> io::Result<ParsedHead> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut request = httparse::Request::new(&mut headers);
    let _parsed_len = match request.parse(buffer) {
        Ok(httparse::Status::Complete(len)) => len,
        Ok(httparse::Status::Partial) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "incomplete HTTP request",
            ));
        }
        Err(err) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid HTTP request: {err}"),
            ));
        }
    };

    let mut parsed_headers = Vec::new();
    let mut content_length = 0_usize;
    let mut saw_transfer_encoding = false;

    for header in request.headers.iter() {
        let name = header.name.to_string();
        if name.eq_ignore_ascii_case("content-length") {
            let value = std::str::from_utf8(header.value).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid content-length header: {err}"),
                )
            })?;
            content_length = value.parse::<usize>().map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid content-length header: {err}"),
                )
            })?;
        }
        if name.eq_ignore_ascii_case("transfer-encoding") {
            saw_transfer_encoding = true;
        }
        parsed_headers.push((name, header.value.to_vec()));
    }

    if saw_transfer_encoding {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunked transfer encoding is not supported",
        ));
    }

    Ok(ParsedHead {
        request: ParsedRequest {
            method: request
                .method
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing HTTP method"))?
                .to_string(),
            path: request
                .path
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing HTTP path"))?
                .to_string(),
            version: request.version.unwrap_or(1),
            headers: parsed_headers,
            body: Vec::new(),
        },
        body_target_len: content_length.min(max_body_bytes.saturating_add(1)),
    })
}

async fn execute_request(
    app: axum::Router,
    request: ParsedRequest,
) -> io::Result<SerializedResponse> {
    let builder = Request::builder()
        .method(request.method.as_str())
        .uri(request.path.as_str())
        .version(match request.version {
            0 => Version::HTTP_10,
            _ => Version::HTTP_11,
        });
    let mut http_request = builder
        .body(Body::from(request.body))
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

    let headers = http_request.headers_mut();
    for (name, value) in request.headers {
        let header_name = HeaderName::from_str(&name).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid HTTP header name {name}: {err}"),
            )
        })?;
        let header_value = HeaderValue::from_bytes(&value).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid HTTP header value for {name}: {err}"),
            )
        })?;
        headers.append(header_name, header_value);
    }

    let response = app.oneshot(http_request).await.map_err(io::Error::other)?;
    serialize_response(response).await
}

async fn serialize_response(response: axum::response::Response) -> io::Result<SerializedResponse> {
    let (parts, body) = response.into_parts();
    let body = to_bytes(body, usize::MAX)
        .await
        .map_err(io::Error::other)?
        .to_vec();

    let mut headers = Vec::new();
    let mut saw_content_length = false;
    let mut saw_connection = false;

    for (name, value) in &parts.headers {
        if name.as_str().eq_ignore_ascii_case("content-length") {
            saw_content_length = true;
        }
        if name.as_str().eq_ignore_ascii_case("connection") {
            saw_connection = true;
        }
        headers.push((name.as_str().to_string(), value.as_bytes().to_vec()));
    }

    if !saw_content_length {
        headers.push((
            "content-length".to_string(),
            body.len().to_string().into_bytes(),
        ));
    }
    if !saw_connection {
        headers.push(("connection".to_string(), b"close".to_vec()));
    }

    Ok(SerializedResponse {
        status: parts.status,
        headers,
        body,
    })
}

fn write_http_response(stream: &mut TcpStream, response: SerializedResponse) -> io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {} {}\r\n",
        response.status.as_u16(),
        response.status.canonical_reason().unwrap_or("OK"),
    )?;
    for (name, value) in response.headers {
        stream.write_all(name.as_bytes())?;
        stream.write_all(b": ")?;
        stream.write_all(&value)?;
        stream.write_all(b"\r\n")?;
    }
    stream.write_all(b"\r\n")?;
    stream.write_all(&response.body)?;
    stream.flush()
}

fn write_plain_response(
    stream: &mut TcpStream,
    status: StatusCode,
    message: &str,
) -> io::Result<()> {
    let body = message.as_bytes();
    write!(
        stream,
        "HTTP/1.1 {} {}\r\ncontent-length: {}\r\ncontent-type: text/plain; charset=utf-8\r\nconnection: close\r\n\r\n",
        status.as_u16(),
        status.canonical_reason().unwrap_or("Error"),
        body.len(),
    )?;
    stream.write_all(body)?;
    stream.flush()
}

fn header_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| pos + 4)
}

fn read_env<T>(key: &str) -> io::Result<Option<T>>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    match env::var(key) {
        Ok(value) => value.parse::<T>().map(Some).map_err(|err| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("invalid {key}: {err}"))
        }),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{key} must be valid UTF-8"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_request_head_extracts_method_headers_and_body_limit() {
        let raw = concat!(
            "POST / HTTP/1.1\r\n",
            "Host: localhost\r\n",
            "Content-Type: application/x-amz-json-1.1\r\n",
            "Content-Length: 5\r\n",
            "\r\n",
            "hello",
        );

        let parsed = parse_request_head(raw.as_bytes(), 1024).unwrap();

        assert_eq!(parsed.request.method, "POST");
        assert_eq!(parsed.request.path, "/");
        assert_eq!(parsed.request.version, 1);
        assert_eq!(parsed.body_target_len, 5);
        assert_eq!(parsed.request.headers.len(), 3);
    }

    #[test]
    fn parse_request_head_caps_body_reads_at_limit_plus_one() {
        let raw = concat!(
            "POST / HTTP/1.1\r\n",
            "Host: localhost\r\n",
            "Content-Length: 999\r\n",
            "\r\n",
        );

        let parsed = parse_request_head(raw.as_bytes(), 8).unwrap();

        assert_eq!(parsed.body_target_len, 9);
    }

    #[test]
    fn header_end_finds_crlf_boundary() {
        assert_eq!(header_end(b"GET / HTTP/1.1\r\n\r\n"), Some(18));
        assert_eq!(header_end(b"GET / HTTP/1.1\r\n"), None);
    }
}
