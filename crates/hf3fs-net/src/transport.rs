use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use hf3fs_types::Address;

use crate::error::NetError;
use crate::message::{MessageHeader, MESSAGE_HEADER_SIZE, MESSAGE_MAX_SIZE};
use crate::socket::Socket;

// ---------------------------------------------------------------------------
// AsyncConnector
// ---------------------------------------------------------------------------

/// Trait for types that can establish new connections to a remote address.
#[async_trait]
pub trait AsyncConnector<S: Socket>: Send + Sync {
    async fn connect(&self, addr: Address) -> Result<S, NetError>;
}

// ---------------------------------------------------------------------------
// Message framing helpers
// ---------------------------------------------------------------------------

/// Frame a payload with a `MessageHeader` and send it over a socket.
///
/// The on-wire format is:
/// ```text
/// [checksum: 4 bytes LE][size: 4 bytes LE][payload: `size` bytes]
/// ```
///
/// The checksum is computed as CRC32C with the serde magic number in the low
/// byte (see `MessageHeader::for_payload`).
pub async fn send_message<S: Socket>(socket: &S, payload: &[u8]) -> Result<(), NetError> {
    if payload.len() > MESSAGE_MAX_SIZE {
        return Err(NetError::MessageTooLarge {
            size: payload.len(),
            max: MESSAGE_MAX_SIZE,
        });
    }

    let header = MessageHeader::for_payload(payload, false);

    let mut frame = Vec::with_capacity(MESSAGE_HEADER_SIZE + payload.len());
    frame.extend_from_slice(&header.to_bytes());
    frame.extend_from_slice(payload);

    socket.send(Bytes::from(frame)).await
}

/// Read a framed message from a socket and validate its header.
///
/// Returns the payload bytes (without the header). The checksum and magic
/// number are verified; any mismatch returns an appropriate `NetError`.
///
/// This function expects the `Socket::recv` implementation to return a
/// complete framed message (header + payload) in a single call. For
/// stream-oriented transports (e.g. TCP), the transport-specific adapter
/// should handle buffering and length-delimited reads before calling this,
/// or use `recv_message_streaming` instead.
pub async fn recv_message<S: Socket>(socket: &S) -> Result<Bytes, NetError> {
    let data = socket.recv().await?;

    if data.is_empty() {
        return Err(NetError::ConnectionClosed);
    }

    if data.len() < MESSAGE_HEADER_SIZE {
        return Err(NetError::IncompleteHeader {
            need: MESSAGE_HEADER_SIZE,
            have: data.len(),
        });
    }

    let header_bytes: [u8; MESSAGE_HEADER_SIZE] = data[..MESSAGE_HEADER_SIZE]
        .try_into()
        .expect("slice length verified above");
    let header = MessageHeader::from_bytes(&header_bytes);

    let payload = &data[MESSAGE_HEADER_SIZE..];

    // Verify the declared size matches what we received.
    let declared_size = header.size as usize;
    if payload.len() < declared_size {
        return Err(NetError::IncompleteHeader {
            need: MESSAGE_HEADER_SIZE + declared_size,
            have: data.len(),
        });
    }

    let payload = &payload[..declared_size];
    header.validate(payload)?;

    Ok(Bytes::copy_from_slice(payload))
}

// ---------------------------------------------------------------------------
// Transport (connection cache)
// ---------------------------------------------------------------------------

/// Manages a cache of open connections keyed by remote address.
///
/// Modeled after the C++ `TransportPool` but simplified: one connection per
/// address. For production use with connection pooling / sharding, this can
/// be extended.
pub struct Transport<S: Socket> {
    connections: DashMap<Address, Arc<S>>,
}

impl<S: Socket> Transport<S> {
    pub fn new() -> Self {
        Self {
            connections: DashMap::new(),
        }
    }

    /// Retrieve an existing connection or establish a new one via `connector`.
    pub async fn get_or_connect(
        &self,
        addr: Address,
        connector: &(dyn AsyncConnector<S> + '_),
    ) -> Result<Arc<S>, NetError> {
        // Fast path: return cached connection.
        if let Some(entry) = self.connections.get(&addr) {
            return Ok(Arc::clone(entry.value()));
        }

        // Slow path: connect and cache.
        let socket = connector.connect(addr).await?;
        let arc = Arc::new(socket);
        self.connections.insert(addr, Arc::clone(&arc));
        Ok(arc)
    }

    /// Remove a connection from the cache.
    pub fn remove(&self, addr: &Address) {
        self.connections.remove(addr);
    }

    /// Remove all connections from the cache.
    pub fn clear(&self) {
        self.connections.clear();
    }

    /// Return the number of cached connections.
    pub fn len(&self) -> usize {
        self.connections.len()
    }

    /// Return whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }
}

impl<S: Socket> Default for Transport<S> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use hf3fs_types::{Address, AddressType};
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // -----------------------------------------------------------------------
    // Mock socket for transport tests
    // -----------------------------------------------------------------------

    struct MockSocket {
        peer: Address,
        /// Bytes recorded from send() calls.
        sent: Mutex<Vec<Bytes>>,
        /// Pre-configured data returned by recv().
        recv_data: Mutex<Vec<Bytes>>,
    }

    impl MockSocket {
        fn new(peer: Address) -> Self {
            Self {
                peer,
                sent: Mutex::new(Vec::new()),
                recv_data: Mutex::new(Vec::new()),
            }
        }

        fn with_recv_data(peer: Address, data: Vec<Bytes>) -> Self {
            Self {
                peer,
                sent: Mutex::new(Vec::new()),
                recv_data: Mutex::new(data),
            }
        }

        fn take_sent(&self) -> Vec<Bytes> {
            std::mem::take(&mut *self.sent.lock())
        }
    }

    #[async_trait]
    impl Socket for MockSocket {
        async fn send(&self, data: Bytes) -> Result<(), NetError> {
            self.sent.lock().push(data);
            Ok(())
        }
        async fn recv(&self) -> Result<Bytes, NetError> {
            let mut queue = self.recv_data.lock();
            if queue.is_empty() {
                Err(NetError::ConnectionClosed)
            } else {
                Ok(queue.remove(0))
            }
        }
        fn peer_addr(&self) -> Address {
            self.peer
        }
        fn local_addr(&self) -> Address {
            Address::from_octets(127, 0, 0, 1, 0, AddressType::TCP)
        }
        async fn close(&self) {}
    }

    struct MockConnector {
        call_count: AtomicUsize,
    }

    #[async_trait]
    impl AsyncConnector<MockSocket> for MockConnector {
        async fn connect(&self, addr: Address) -> Result<MockSocket, NetError> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(MockSocket::new(addr))
        }
    }

    // -----------------------------------------------------------------------
    // Transport cache tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_get_or_connect_caches() {
        let transport = Transport::<MockSocket>::new();
        let connector = MockConnector {
            call_count: AtomicUsize::new(0),
        };
        let addr = Address::from_octets(10, 0, 0, 1, 8080, AddressType::TCP);

        let s1 = transport.get_or_connect(addr, &connector).await.unwrap();
        let s2 = transport.get_or_connect(addr, &connector).await.unwrap();

        assert_eq!(connector.call_count.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&s1, &s2));
    }

    #[tokio::test]
    async fn test_remove() {
        let transport = Transport::<MockSocket>::new();
        let connector = MockConnector {
            call_count: AtomicUsize::new(0),
        };
        let addr = Address::from_octets(10, 0, 0, 1, 8080, AddressType::TCP);

        let _ = transport.get_or_connect(addr, &connector).await.unwrap();
        transport.remove(&addr);
        let _ = transport.get_or_connect(addr, &connector).await.unwrap();

        assert_eq!(connector.call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_clear() {
        let transport = Transport::<MockSocket>::new();
        let connector = MockConnector {
            call_count: AtomicUsize::new(0),
        };

        let addr1 = Address::from_octets(10, 0, 0, 1, 8080, AddressType::TCP);
        let addr2 = Address::from_octets(10, 0, 0, 2, 8080, AddressType::TCP);

        let _ = transport.get_or_connect(addr1, &connector).await.unwrap();
        let _ = transport.get_or_connect(addr2, &connector).await.unwrap();
        assert_eq!(transport.len(), 2);

        transport.clear();
        assert!(transport.is_empty());
    }

    // -----------------------------------------------------------------------
    // Message framing tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_send_message_framing() {
        let addr = Address::from_octets(10, 0, 0, 1, 8080, AddressType::TCP);
        let socket = MockSocket::new(addr);
        let payload = b"hello, world!";

        send_message(&socket, payload).await.unwrap();

        let sent = socket.take_sent();
        assert_eq!(sent.len(), 1);

        let frame = &sent[0];
        assert_eq!(frame.len(), MESSAGE_HEADER_SIZE + payload.len());

        // Parse the header back.
        let header_bytes: [u8; MESSAGE_HEADER_SIZE] =
            frame[..MESSAGE_HEADER_SIZE].try_into().unwrap();
        let header = MessageHeader::from_bytes(&header_bytes);
        assert!(header.is_serde_message());
        assert!(!header.is_compressed());
        assert_eq!(header.size as usize, payload.len());

        // Verify the payload.
        assert_eq!(&frame[MESSAGE_HEADER_SIZE..], payload);

        // Verify the checksum.
        assert!(header.validate(payload).is_ok());
    }

    #[tokio::test]
    async fn test_send_message_empty_payload() {
        let addr = Address::from_octets(10, 0, 0, 1, 8080, AddressType::TCP);
        let socket = MockSocket::new(addr);

        send_message(&socket, b"").await.unwrap();

        let sent = socket.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].len(), MESSAGE_HEADER_SIZE);
    }

    #[tokio::test]
    async fn test_recv_message_valid() {
        let payload = b"response data";
        let header = MessageHeader::for_payload(payload, false);

        let mut frame = Vec::with_capacity(MESSAGE_HEADER_SIZE + payload.len());
        frame.extend_from_slice(&header.to_bytes());
        frame.extend_from_slice(payload);

        let addr = Address::from_octets(10, 0, 0, 1, 8080, AddressType::TCP);
        let socket = MockSocket::with_recv_data(addr, vec![Bytes::from(frame)]);

        let received = recv_message(&socket).await.unwrap();
        assert_eq!(received.as_ref(), payload);
    }

    #[tokio::test]
    async fn test_recv_message_empty_frame() {
        let addr = Address::from_octets(10, 0, 0, 1, 8080, AddressType::TCP);
        let socket = MockSocket::with_recv_data(addr, vec![Bytes::new()]);

        let result = recv_message(&socket).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), NetError::ConnectionClosed));
    }

    #[tokio::test]
    async fn test_recv_message_incomplete_header() {
        let addr = Address::from_octets(10, 0, 0, 1, 8080, AddressType::TCP);
        let socket = MockSocket::with_recv_data(addr, vec![Bytes::from_static(&[0, 1, 2])]);

        let result = recv_message(&socket).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            NetError::IncompleteHeader { need: 8, have: 3 }
        ));
    }

    #[tokio::test]
    async fn test_recv_message_bad_checksum() {
        let payload = b"test data";
        let mut header = MessageHeader::for_payload(payload, false);
        // Corrupt the checksum upper bits but keep magic valid
        header.checksum ^= 0xFF00_0000;

        let mut frame = Vec::new();
        frame.extend_from_slice(&header.to_bytes());
        frame.extend_from_slice(payload);

        let addr = Address::from_octets(10, 0, 0, 1, 8080, AddressType::TCP);
        let socket = MockSocket::with_recv_data(addr, vec![Bytes::from(frame)]);

        let result = recv_message(&socket).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            NetError::ChecksumMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn test_send_recv_roundtrip() {
        let addr = Address::from_octets(10, 0, 0, 1, 8080, AddressType::TCP);
        let payload = b"roundtrip payload test";

        // Send into a mock socket and capture the frame.
        let send_socket = MockSocket::new(addr);
        send_message(&send_socket, payload).await.unwrap();
        let sent = send_socket.take_sent();

        // Feed the captured frame into recv_message.
        let recv_socket = MockSocket::with_recv_data(addr, sent);
        let received = recv_message(&recv_socket).await.unwrap();

        assert_eq!(received.as_ref(), payload);
    }

    #[tokio::test]
    async fn test_send_recv_roundtrip_large() {
        let addr = Address::from_octets(10, 0, 0, 1, 8080, AddressType::TCP);
        let payload = vec![0xABu8; 65536];

        let send_socket = MockSocket::new(addr);
        send_message(&send_socket, &payload).await.unwrap();
        let sent = send_socket.take_sent();

        let recv_socket = MockSocket::with_recv_data(addr, sent);
        let received = recv_message(&recv_socket).await.unwrap();

        assert_eq!(received.as_ref(), payload.as_slice());
    }

    #[tokio::test]
    async fn test_recv_message_bad_magic() {
        // Create a frame with a non-serde checksum
        let mut frame = vec![0u8; MESSAGE_HEADER_SIZE];
        // checksum = 0x12345678, size = 0
        frame[0..4].copy_from_slice(&0x1234_5678u32.to_le_bytes());
        frame[4..8].copy_from_slice(&0u32.to_le_bytes());

        let addr = Address::from_octets(10, 0, 0, 1, 8080, AddressType::TCP);
        let socket = MockSocket::with_recv_data(addr, vec![Bytes::from(frame)]);

        let result = recv_message(&socket).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            NetError::InvalidMagic(0x78)
        ));
    }

    #[tokio::test]
    async fn test_send_message_too_large() {
        let addr = Address::from_octets(10, 0, 0, 1, 8080, AddressType::TCP);
        let socket = MockSocket::new(addr);
        let payload = vec![0u8; MESSAGE_MAX_SIZE + 1];

        let result = send_message(&socket, &payload).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            NetError::MessageTooLarge { .. }
        ));
    }
}
