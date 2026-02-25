//! TCP transport implementation for the 3FS networking layer.
//!
//! Provides [`TcpSocket`] and [`TcpListener`] which wrap Tokio's TCP primitives
//! and implement the [`hf3fs_net::Socket`] and [`hf3fs_net::AsyncConnector`] traits.
//!
//! The socket splits a `TcpStream` into independent read/write halves so that
//! sending and receiving can proceed concurrently without holding a single lock
//! over the entire stream.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use hf3fs_net::error::SocketError;
use hf3fs_net::socket::Socket;
use hf3fs_net::transport::AsyncConnector;
use hf3fs_types::{Address, AddressType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Helpers for converting between hf3fs Address and std SocketAddr
// ---------------------------------------------------------------------------

/// Convert an [`Address`] into a [`SocketAddr`].
///
/// Only [`AddressType::TCP`] addresses are meaningful here, but we perform the
/// conversion for any type so callers can decide policy.
fn address_to_socket_addr(addr: &Address) -> SocketAddr {
    let octets = addr.octets();
    SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::new(octets[0], octets[1], octets[2], octets[3]),
        addr.port,
    ))
}

/// Convert a [`SocketAddr`] into an [`Address`] with [`AddressType::TCP`].
fn socket_addr_to_address(sa: SocketAddr) -> Address {
    match sa {
        SocketAddr::V4(v4) => {
            let octets = v4.ip().octets();
            Address::from_octets(octets[0], octets[1], octets[2], octets[3], v4.port(), AddressType::TCP)
        }
        SocketAddr::V6(v6) => {
            // Map IPv6-mapped-IPv4 addresses; for anything else use zeroed IP.
            if let Some(v4) = v6.ip().to_ipv4_mapped() {
                let octets = v4.octets();
                Address::from_octets(octets[0], octets[1], octets[2], octets[3], v6.port(), AddressType::TCP)
            } else {
                // Lossy: we only support IPv4 in the Address struct.
                Address::new(0, v6.port(), AddressType::TCP)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// TcpSocket
// ---------------------------------------------------------------------------

/// A TCP socket wrapping a Tokio [`TcpStream`](tokio::net::TcpStream).
///
/// The underlying stream is split into independent read and write halves that
/// are each protected by an async mutex, allowing concurrent send/recv from
/// different tasks.
pub struct TcpSocket {
    reader: Arc<Mutex<OwnedReadHalf>>,
    writer: Arc<Mutex<OwnedWriteHalf>>,
    peer_addr: Address,
    local_addr: Address,
}

impl TcpSocket {
    /// Wrap an already-connected [`tokio::net::TcpStream`].
    pub fn from_stream(stream: tokio::net::TcpStream) -> Result<Self, SocketError> {
        let peer_sa = stream.peer_addr()?;
        let local_sa = stream.local_addr()?;

        let peer_addr = socket_addr_to_address(peer_sa);
        let local_addr = socket_addr_to_address(local_sa);

        let (read_half, write_half) = stream.into_split();

        Ok(Self {
            reader: Arc::new(Mutex::new(read_half)),
            writer: Arc::new(Mutex::new(write_half)),
            peer_addr,
            local_addr,
        })
    }

    /// Send raw bytes over the socket.
    ///
    /// This is a standalone method so that `TcpSocket` is usable even without
    /// going through the `Socket` trait (e.g. if hf3fs-net is not fully ready).
    pub async fn send_bytes(&self, data: &[u8]) -> Result<(), SocketError> {
        let mut writer = self.writer.lock().await;
        writer.write_all(data).await?;
        writer.flush().await?;
        Ok(())
    }

    /// Receive up to `buf.len()` bytes from the socket.
    ///
    /// Returns the number of bytes read. A return value of `0` means EOF
    /// (the peer closed the connection).
    pub async fn recv_bytes(&self, buf: &mut [u8]) -> Result<usize, SocketError> {
        let mut reader = self.reader.lock().await;
        let n = reader.read(buf).await?;
        Ok(n)
    }

    /// Return the remote peer address.
    pub fn peer_address(&self) -> Address {
        self.peer_addr
    }

    /// Return the local bind address.
    pub fn local_address(&self) -> Address {
        self.local_addr
    }

    /// Shut down the socket.
    pub async fn shutdown(&self) {
        // Attempt to shut down the write half; ignore errors (e.g. already closed).
        let mut writer = self.writer.lock().await;
        let _ = writer.shutdown().await;
    }
}

impl std::fmt::Debug for TcpSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpSocket")
            .field("peer_addr", &self.peer_addr)
            .field("local_addr", &self.local_addr)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Socket trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Socket for TcpSocket {
    async fn send(&self, data: Bytes) -> Result<(), SocketError> {
        self.send_bytes(&data).await
    }

    async fn recv(&self) -> Result<Bytes, SocketError> {
        // Use a reasonable default buffer size for trait-based recv.
        let mut buf = BytesMut::zeroed(64 * 1024);
        let n = self.recv_bytes(&mut buf).await?;
        if n == 0 {
            return Err(SocketError::ConnectionClosed);
        }
        buf.truncate(n);
        Ok(buf.freeze())
    }

    fn peer_addr(&self) -> Address {
        self.peer_addr
    }

    fn local_addr(&self) -> Address {
        self.local_addr
    }

    async fn close(&self) {
        self.shutdown().await;
    }
}

// ---------------------------------------------------------------------------
// TcpListener
// ---------------------------------------------------------------------------

/// A TCP listener wrapping [`tokio::net::TcpListener`].
///
/// Accepts incoming TCP connections and yields [`TcpSocket`] instances.
pub struct TcpListener {
    inner: tokio::net::TcpListener,
    local_addr: Address,
}

impl TcpListener {
    /// Bind to the given [`Address`].
    ///
    /// The address type is ignored; the socket is always TCP.
    pub async fn bind(addr: Address) -> Result<Self, SocketError> {
        let sa = address_to_socket_addr(&addr);
        let listener = tokio::net::TcpListener::bind(sa).await?;

        // Resolve the actual local address (port may differ if 0 was requested).
        let actual_sa = listener.local_addr()?;
        let local_addr = socket_addr_to_address(actual_sa);

        tracing::info!(%local_addr, "TCP listener bound");

        Ok(Self {
            inner: listener,
            local_addr,
        })
    }

    /// Bind to the given [`SocketAddr`] directly.
    pub async fn bind_socket_addr(sa: SocketAddr) -> Result<Self, SocketError> {
        let listener = tokio::net::TcpListener::bind(sa).await?;
        let actual_sa = listener.local_addr()?;
        let local_addr = socket_addr_to_address(actual_sa);

        tracing::info!(%local_addr, "TCP listener bound");

        Ok(Self {
            inner: listener,
            local_addr,
        })
    }

    /// Accept the next incoming connection.
    pub async fn accept(&self) -> Result<TcpSocket, SocketError> {
        let (stream, peer_sa) = self.inner.accept().await?;

        tracing::debug!(peer = %peer_sa, "accepted TCP connection");

        TcpSocket::from_stream(stream)
    }

    /// Return the local address the listener is bound to.
    pub fn local_address(&self) -> Address {
        self.local_addr
    }

    /// Return the local address as a [`SocketAddr`].
    pub fn local_socket_addr(&self) -> Result<SocketAddr, SocketError> {
        Ok(self.inner.local_addr()?)
    }
}

impl std::fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpListener")
            .field("local_addr", &self.local_addr)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// connect() free function
// ---------------------------------------------------------------------------

/// Connect to a remote address and return a [`TcpSocket`].
pub async fn connect(addr: Address) -> Result<TcpSocket, SocketError> {
    let sa = address_to_socket_addr(&addr);
    tracing::debug!(%addr, "connecting via TCP");
    let stream = tokio::net::TcpStream::connect(sa).await?;
    TcpSocket::from_stream(stream)
}

/// Connect to a [`SocketAddr`] directly.
pub async fn connect_socket_addr(sa: SocketAddr) -> Result<TcpSocket, SocketError> {
    let stream = tokio::net::TcpStream::connect(sa).await?;
    TcpSocket::from_stream(stream)
}

// ---------------------------------------------------------------------------
// AsyncConnector implementation
// ---------------------------------------------------------------------------

/// A connector that creates [`TcpSocket`] instances by opening TCP connections.
///
/// Implements the [`AsyncConnector`] trait from `hf3fs-net` so it can be used
/// with [`hf3fs_net::Transport`].
#[derive(Debug, Clone, Default)]
pub struct TcpConnector;

impl TcpConnector {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AsyncConnector<TcpSocket> for TcpConnector {
    async fn connect(&self, addr: Address) -> Result<TcpSocket, SocketError> {
        crate::connect(addr).await
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use hf3fs_types::AddressType;

    #[test]
    fn test_address_conversion_roundtrip() {
        let addr = Address::from_octets(192, 168, 1, 42, 9090, AddressType::TCP);
        let sa = address_to_socket_addr(&addr);
        assert_eq!(sa.to_string(), "192.168.1.42:9090");

        let back = socket_addr_to_address(sa);
        assert_eq!(back.octets(), [192, 168, 1, 42]);
        assert_eq!(back.port, 9090);
        assert_eq!(back.addr_type, AddressType::TCP);
    }

    #[test]
    fn test_address_v6_mapped() {
        let sa: SocketAddr = "[::ffff:10.0.0.1]:8080".parse().unwrap();
        let addr = socket_addr_to_address(sa);
        assert_eq!(addr.octets(), [10, 0, 0, 1]);
        assert_eq!(addr.port, 8080);
    }

    #[tokio::test]
    async fn test_listener_bind_and_local_addr() {
        let bind_addr = Address::from_octets(127, 0, 0, 1, 0, AddressType::TCP);
        let listener = TcpListener::bind(bind_addr).await.unwrap();

        let local = listener.local_address();
        assert_eq!(local.octets(), [127, 0, 0, 1]);
        // Port should have been assigned by the OS (nonzero).
        assert_ne!(local.port, 0);
    }

    #[tokio::test]
    async fn test_connect_and_accept() {
        let bind_addr = Address::from_octets(127, 0, 0, 1, 0, AddressType::TCP);
        let listener = TcpListener::bind(bind_addr).await.unwrap();
        let server_addr = listener.local_address();

        // Spawn an acceptor task.
        let accept_handle = tokio::spawn(async move {
            listener.accept().await.unwrap()
        });

        // Connect from the client side.
        let client = connect(server_addr).await.unwrap();

        // Wait for the server side to accept.
        let server_socket = accept_handle.await.unwrap();

        // Verify addresses match up.
        assert_eq!(client.peer_address(), server_addr);
        assert_eq!(server_socket.local_address(), server_addr);
        assert_eq!(server_socket.peer_address(), client.local_address());
    }

    #[tokio::test]
    async fn test_send_recv_data() {
        let bind_addr = Address::from_octets(127, 0, 0, 1, 0, AddressType::TCP);
        let listener = TcpListener::bind(bind_addr).await.unwrap();
        let server_addr = listener.local_address();

        let accept_handle = tokio::spawn(async move {
            listener.accept().await.unwrap()
        });

        let client = connect(server_addr).await.unwrap();
        let server_socket = accept_handle.await.unwrap();

        // Client sends data, server receives.
        let payload = b"hello from the client";
        client.send_bytes(payload).await.unwrap();

        let mut buf = vec![0u8; 1024];
        let n = server_socket.recv_bytes(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], payload);

        // Server sends response, client receives.
        let response = b"response from server";
        server_socket.send_bytes(response).await.unwrap();

        let mut buf2 = vec![0u8; 1024];
        let n2 = client.recv_bytes(&mut buf2).await.unwrap();
        assert_eq!(&buf2[..n2], response);
    }

    #[tokio::test]
    async fn test_send_recv_via_trait() {
        // Test using the Socket trait methods.
        let bind_addr = Address::from_octets(127, 0, 0, 1, 0, AddressType::TCP);
        let listener = TcpListener::bind(bind_addr).await.unwrap();
        let server_addr = listener.local_address();

        let accept_handle = tokio::spawn(async move {
            listener.accept().await.unwrap()
        });

        let client = connect(server_addr).await.unwrap();
        let server_socket = accept_handle.await.unwrap();

        // Use the Socket trait methods.
        let data = Bytes::from_static(b"trait-based send");
        Socket::send(&client, data.clone()).await.unwrap();

        let received = Socket::recv(&server_socket).await.unwrap();
        assert_eq!(received, data);
    }

    #[tokio::test]
    async fn test_close_signals_eof() {
        let bind_addr = Address::from_octets(127, 0, 0, 1, 0, AddressType::TCP);
        let listener = TcpListener::bind(bind_addr).await.unwrap();
        let server_addr = listener.local_address();

        let accept_handle = tokio::spawn(async move {
            listener.accept().await.unwrap()
        });

        let client = connect(server_addr).await.unwrap();
        let server_socket = accept_handle.await.unwrap();

        // Close the client side.
        client.shutdown().await;

        // Server should get EOF (0 bytes) or Closed error via trait.
        let mut buf = vec![0u8; 64];
        let n = server_socket.recv_bytes(&mut buf).await.unwrap();
        assert_eq!(n, 0, "expected EOF after peer shutdown");
    }

    #[tokio::test]
    async fn test_large_payload() {
        let bind_addr = Address::from_octets(127, 0, 0, 1, 0, AddressType::TCP);
        let listener = TcpListener::bind(bind_addr).await.unwrap();
        let server_addr = listener.local_address();

        let accept_handle = tokio::spawn(async move {
            listener.accept().await.unwrap()
        });

        let client = connect(server_addr).await.unwrap();
        let server_socket = accept_handle.await.unwrap();

        // Send a large payload (1 MB).
        let payload: Vec<u8> = (0..1_000_000).map(|i| (i % 251) as u8).collect();

        let payload_clone = payload.clone();
        let send_handle = tokio::spawn(async move {
            client.send_bytes(&payload_clone).await.unwrap();
            client.shutdown().await;
        });

        // Receive all data.
        let mut received = Vec::new();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = server_socket.recv_bytes(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            received.extend_from_slice(&buf[..n]);
        }

        send_handle.await.unwrap();

        assert_eq!(received.len(), payload.len());
        assert_eq!(received, payload);
    }

    #[tokio::test]
    async fn test_tcp_connector() {
        let bind_addr = Address::from_octets(127, 0, 0, 1, 0, AddressType::TCP);
        let listener = TcpListener::bind(bind_addr).await.unwrap();
        let server_addr = listener.local_address();

        let accept_handle = tokio::spawn(async move {
            listener.accept().await.unwrap()
        });

        let connector = TcpConnector::new();
        let client = AsyncConnector::connect(&connector, server_addr).await.unwrap();

        let server_socket = accept_handle.await.unwrap();

        // Verify the connection works.
        client.send_bytes(b"via connector").await.unwrap();

        let mut buf = vec![0u8; 256];
        let n = server_socket.recv_bytes(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"via connector");
    }

    #[tokio::test]
    async fn test_connect_refused() {
        // Bind and then drop the listener so the port is closed.
        let bind_addr = Address::from_octets(127, 0, 0, 1, 0, AddressType::TCP);
        let listener = TcpListener::bind(bind_addr).await.unwrap();
        let addr = listener.local_address();
        drop(listener);

        // Connecting should fail.
        let result = connect(addr).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bind_socket_addr_and_connect_socket_addr() {
        let sa: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind_socket_addr(sa).await.unwrap();
        let actual_sa = listener.local_socket_addr().unwrap();

        let accept_handle = tokio::spawn(async move {
            listener.accept().await.unwrap()
        });

        let client = connect_socket_addr(actual_sa).await.unwrap();
        let server_socket = accept_handle.await.unwrap();

        client.send_bytes(b"socket addr api").await.unwrap();

        let mut buf = vec![0u8; 256];
        let n = server_socket.recv_bytes(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"socket addr api");
    }

    #[tokio::test]
    async fn test_bidirectional_concurrent() {
        // Test that send and recv can happen concurrently from both sides.
        let bind_addr = Address::from_octets(127, 0, 0, 1, 0, AddressType::TCP);
        let listener = TcpListener::bind(bind_addr).await.unwrap();
        let server_addr = listener.local_address();

        let accept_handle = tokio::spawn(async move {
            listener.accept().await.unwrap()
        });

        let client = Arc::new(connect(server_addr).await.unwrap());
        let server_socket = Arc::new(accept_handle.await.unwrap());

        // Client sends while server sends concurrently.
        let client_send = {
            let c = Arc::clone(&client);
            tokio::spawn(async move {
                c.send_bytes(b"from client").await.unwrap();
            })
        };

        let server_send = {
            let s = Arc::clone(&server_socket);
            tokio::spawn(async move {
                s.send_bytes(b"from server").await.unwrap();
            })
        };

        client_send.await.unwrap();
        server_send.await.unwrap();

        // Read from both sides.
        let mut buf = vec![0u8; 256];
        let n = server_socket.recv_bytes(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"from client");

        let mut buf2 = vec![0u8; 256];
        let n2 = client.recv_bytes(&mut buf2).await.unwrap();
        assert_eq!(&buf2[..n2], b"from server");
    }
}
