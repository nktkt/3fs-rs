use std::sync::Arc;

use bytes::{Bytes, BufMut, BytesMut};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tracing;

use crate::error::NetError;
use crate::message::{MessageHeader, MESSAGE_HEADER_SIZE};
use crate::service::{ServiceHandler, ServiceRegistry};
use crate::socket::{Listener, Socket};
use crate::transport::{recv_message, send_message};

/// Envelope for an RPC request on the wire.
///
/// After the `MessageHeader` + payload are read, the first 4 bytes of the
/// payload encode:
///   - `service_id: u16 LE`
///   - `method_id:  u16 LE`
///
/// followed by the actual request body.
pub const REQUEST_ENVELOPE_SIZE: usize = 4;

/// RPC server that hosts registered services.
///
/// The server accepts connections from a `Listener`, reads framed RPC
/// messages, dispatches them to the appropriate `ServiceHandler`, and
/// writes back the response. Shutdown is coordinated through `stop()`.
pub struct Server {
    services: Arc<ServiceRegistry>,
    /// Signalled when `stop()` is called to cancel the accept loop.
    shutdown: Arc<Notify>,
    /// Whether the server has been started.
    running: bool,
}

impl Server {
    pub fn new() -> Self {
        Self {
            services: Arc::new(ServiceRegistry::new()),
            shutdown: Arc::new(Notify::new()),
            running: false,
        }
    }

    /// Create a server with the given pre-populated registry.
    pub fn with_registry(registry: ServiceRegistry) -> Self {
        Self {
            services: Arc::new(registry),
            shutdown: Arc::new(Notify::new()),
            running: false,
        }
    }

    /// Register a service handler with this server.
    pub fn register_service(&self, service: Box<dyn ServiceHandler>) {
        self.services.register(service);
    }

    /// Return a reference to the service registry.
    pub fn services(&self) -> &ServiceRegistry {
        &self.services
    }

    /// Start accepting connections from the provided `Listener`.
    ///
    /// This spawns a background task that runs until `stop()` is called.
    /// Each accepted connection is handled in its own spawned task.
    pub fn start<L: Listener + 'static>(&mut self, listener: L) {
        if self.running {
            tracing::warn!("server already running, ignoring duplicate start");
            return;
        }
        self.running = true;

        let services = Arc::clone(&self.services);
        let shutdown = Arc::clone(&self.shutdown);
        let addr = listener.local_addr();

        tracing::info!(%addr, "server starting");

        tokio::spawn(async move {
            Self::accept_loop(listener, services, shutdown).await;
            tracing::info!(%addr, "server accept loop exited");
        });
    }

    /// Stop the server, signaling the accept loop and all active connections.
    pub fn stop(&mut self) {
        if self.running {
            tracing::info!("server stopping");
            self.shutdown.notify_waiters();
            self.running = false;
        }
    }

    /// Return whether the server is running.
    pub fn is_running(&self) -> bool {
        self.running
    }

    // -----------------------------------------------------------------------
    // Internal implementation
    // -----------------------------------------------------------------------

    async fn accept_loop<L: Listener>(
        listener: L,
        services: Arc<ServiceRegistry>,
        shutdown: Arc<Notify>,
    ) {
        let mut tasks = JoinSet::new();

        loop {
            tokio::select! {
                biased;

                _ = shutdown.notified() => {
                    tracing::info!("server shutdown signal received");
                    break;
                }

                result = listener.accept() => {
                    match result {
                        Ok(socket) => {
                            let services = Arc::clone(&services);
                            let shutdown = Arc::clone(&shutdown);
                            tasks.spawn(async move {
                                if let Err(e) = Self::handle_connection(socket, services, shutdown).await {
                                    tracing::debug!("connection handler finished: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("accept error: {}", e);
                            // Brief pause to avoid tight error loops.
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        }
                    }
                }
            }
        }

        // Wait for all active connection tasks to finish.
        tasks.shutdown().await;
    }

    async fn handle_connection<S: Socket>(
        socket: S,
        services: Arc<ServiceRegistry>,
        shutdown: Arc<Notify>,
    ) -> Result<(), NetError> {
        let peer = socket.peer_addr();
        tracing::debug!(%peer, "new connection");

        loop {
            // Check for shutdown between requests.
            let payload = tokio::select! {
                biased;

                _ = shutdown.notified() => {
                    tracing::debug!(%peer, "connection shutdown");
                    return Err(NetError::ShuttingDown);
                }

                result = recv_message(&socket) => {
                    result?
                }
            };

            // Decode the request envelope.
            if payload.len() < REQUEST_ENVELOPE_SIZE {
                tracing::warn!(%peer, "request too small: {} bytes", payload.len());
                continue;
            }

            let service_id =
                u16::from_le_bytes([payload[0], payload[1]]);
            let method_id =
                u16::from_le_bytes([payload[2], payload[3]]);
            let body = payload.slice(REQUEST_ENVELOPE_SIZE..);

            tracing::debug!(
                %peer,
                service_id,
                method_id,
                body_len = body.len(),
                "dispatching request"
            );

            // Look up the service.
            let response = match services.get(service_id) {
                Some(handler) => {
                    match handler.handle(method_id, body).await {
                        Ok(resp) => resp,
                        Err(status) => {
                            // Encode error status as a response: we send the
                            // status code as a u16 LE with an empty body.
                            // The client can detect an error because the
                            // status code will be non-zero.
                            tracing::debug!(
                                %peer,
                                service_id,
                                method_id,
                                status = %status,
                                "handler returned error"
                            );
                            encode_error_response(status.code())
                        }
                    }
                }
                None => {
                    tracing::warn!(%peer, service_id, "service not found");
                    encode_error_response(hf3fs_types::status_code::RPCCode::INVALID_SERVICE_ID)
                }
            };

            // Send the response back.
            send_message(&socket, &response).await?;
        }
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

/// Encode an error response as `[status_code: u16 LE]`.
fn encode_error_response(code: u16) -> Bytes {
    let mut buf = BytesMut::with_capacity(2);
    buf.put_u16_le(code);
    buf.freeze()
}

/// Build the on-wire request envelope: `[service_id: u16 LE][method_id: u16 LE][body...]`.
pub fn encode_request(service_id: u16, method_id: u16, body: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(REQUEST_ENVELOPE_SIZE + body.len());
    buf.put_u16_le(service_id);
    buf.put_u16_le(method_id);
    buf.extend_from_slice(body);
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::ServiceHandler;
    use crate::socket::Listener;
    use async_trait::async_trait;
    use hf3fs_types::{Address, AddressType, Status};
    use parking_lot::Mutex;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};

    // -----------------------------------------------------------------------
    // Mock socket with bidirectional message queues
    // -----------------------------------------------------------------------

    struct MockSocket {
        peer: Address,
        local: Address,
        /// Messages "sent" by this socket (collected for assertions).
        outbox: Mutex<Vec<Bytes>>,
        /// Messages to be "received" by this socket.
        inbox: Mutex<VecDeque<Bytes>>,
        closed: AtomicBool,
    }

    impl MockSocket {
        fn new(peer: Address, local: Address, inbox: VecDeque<Bytes>) -> Self {
            Self {
                peer,
                local,
                outbox: Mutex::new(Vec::new()),
                inbox: Mutex::new(inbox),
                closed: AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl Socket for MockSocket {
        async fn send(&self, data: Bytes) -> Result<(), NetError> {
            if self.closed.load(Ordering::SeqCst) {
                return Err(NetError::ConnectionClosed);
            }
            self.outbox.lock().push(data);
            Ok(())
        }
        async fn recv(&self) -> Result<Bytes, NetError> {
            if self.closed.load(Ordering::SeqCst) {
                return Err(NetError::ConnectionClosed);
            }
            let mut inbox = self.inbox.lock();
            match inbox.pop_front() {
                Some(data) => Ok(data),
                None => Err(NetError::ConnectionClosed),
            }
        }
        fn peer_addr(&self) -> Address {
            self.peer
        }
        fn local_addr(&self) -> Address {
            self.local
        }
        async fn close(&self) {
            self.closed.store(true, Ordering::SeqCst);
        }
    }

    // -----------------------------------------------------------------------
    // Helper: build a framed request
    // -----------------------------------------------------------------------

    fn build_framed_request(service_id: u16, method_id: u16, body: &[u8]) -> Bytes {
        let envelope = encode_request(service_id, method_id, body);
        let header = MessageHeader::for_payload(&envelope, false);
        let mut frame = Vec::with_capacity(MESSAGE_HEADER_SIZE + envelope.len());
        frame.extend_from_slice(&header.to_bytes());
        frame.extend_from_slice(&envelope);
        Bytes::from(frame)
    }

    /// Parse a framed response that was sent by the server.
    fn parse_framed_response(frame: &Bytes) -> Bytes {
        assert!(frame.len() >= MESSAGE_HEADER_SIZE);
        let header_bytes: [u8; MESSAGE_HEADER_SIZE] =
            frame[..MESSAGE_HEADER_SIZE].try_into().unwrap();
        let header = MessageHeader::from_bytes(&header_bytes);
        let payload = &frame[MESSAGE_HEADER_SIZE..MESSAGE_HEADER_SIZE + header.size as usize];
        Bytes::copy_from_slice(payload)
    }

    // -----------------------------------------------------------------------
    // Test service handler
    // -----------------------------------------------------------------------

    struct EchoService;

    #[async_trait]
    impl ServiceHandler for EchoService {
        fn service_id(&self) -> u16 {
            1
        }
        fn service_name(&self) -> &str {
            "echo"
        }
        async fn handle(&self, _method_id: u16, request: Bytes) -> Result<Bytes, Status> {
            Ok(request)
        }
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_encode_request() {
        let req = encode_request(1, 2, b"hello");
        assert_eq!(req.len(), 4 + 5);
        assert_eq!(u16::from_le_bytes([req[0], req[1]]), 1);
        assert_eq!(u16::from_le_bytes([req[2], req[3]]), 2);
        assert_eq!(&req[4..], b"hello");
    }

    #[test]
    fn test_server_register_service() {
        let server = Server::new();
        server.register_service(Box::new(EchoService));
        assert!(server.services().get(1).is_some());
        assert_eq!(server.services().get(1).unwrap().service_name(), "echo");
    }

    #[tokio::test]
    async fn test_handle_connection_echo() {
        // Build a single request frame
        let request_frame = build_framed_request(1, 0, b"ping");

        let peer = Address::from_octets(10, 0, 0, 1, 5000, AddressType::TCP);
        let local = Address::from_octets(0, 0, 0, 0, 9000, AddressType::TCP);
        let inbox = VecDeque::from(vec![request_frame]);

        let socket = MockSocket::new(peer, local, inbox);

        let registry = ServiceRegistry::new();
        registry.register(Box::new(EchoService));

        let shutdown = Arc::new(Notify::new());
        let services = Arc::new(registry);

        // The connection handler will process one request then get
        // ConnectionClosed when trying to read the next.
        let result = Server::handle_connection(socket, services, shutdown).await;
        // It should return ConnectionClosed since the inbox is exhausted.
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handle_connection_service_not_found() {
        // Request for service_id=99 which is not registered.
        let request_frame = build_framed_request(99, 0, b"test");

        let peer = Address::from_octets(10, 0, 0, 1, 5000, AddressType::TCP);
        let local = Address::from_octets(0, 0, 0, 0, 9000, AddressType::TCP);
        let inbox = VecDeque::from(vec![request_frame]);

        let socket = MockSocket::new(peer, local, inbox);

        let registry = ServiceRegistry::new();
        registry.register(Box::new(EchoService));

        let shutdown = Arc::new(Notify::new());
        let services = Arc::new(registry);

        let result = Server::handle_connection(socket, services, shutdown).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_server_with_registry() {
        let registry = ServiceRegistry::new();
        registry.register(Box::new(EchoService));

        let server = Server::with_registry(registry);
        assert!(server.services().get(1).is_some());
    }

    #[tokio::test]
    async fn test_encode_error_response() {
        let resp = encode_error_response(
            hf3fs_types::status_code::RPCCode::INVALID_SERVICE_ID,
        );
        assert_eq!(resp.len(), 2);
        let code = u16::from_le_bytes([resp[0], resp[1]]);
        assert_eq!(code, hf3fs_types::status_code::RPCCode::INVALID_SERVICE_ID);
    }
}
