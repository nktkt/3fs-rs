use async_trait::async_trait;
use bytes::Bytes;
use hf3fs_types::Address;

use crate::error::NetError;

/// Trait representing an abstract network socket.
///
/// Implementations may use TCP, RDMA, Unix domain sockets, or in-memory
/// channels for testing. Each implementation lives in its own crate
/// (e.g. `hf3fs-net-tcp`, `hf3fs-net-rdma`).
#[async_trait]
pub trait Socket: Send + Sync + 'static {
    /// Send data over the socket.
    async fn send(&self, data: Bytes) -> Result<(), NetError>;

    /// Receive data from the socket.
    ///
    /// Returns the received bytes. An empty `Bytes` or an error of
    /// `NetError::ConnectionClosed` indicates the connection was closed.
    async fn recv(&self) -> Result<Bytes, NetError>;

    /// Return the remote peer address.
    fn peer_addr(&self) -> Address;

    /// Return the local bind address.
    fn local_addr(&self) -> Address;

    /// Close the socket gracefully.
    async fn close(&self);
}

/// Trait for accepting incoming connections.
///
/// A `Listener` is bound to a local address and yields connected `Socket`
/// instances. The concrete implementation is transport-specific.
#[async_trait]
pub trait Listener: Send + Sync + 'static {
    /// The type of socket produced when a connection is accepted.
    type Socket: Socket;

    /// Accept the next incoming connection.
    ///
    /// This method blocks (asynchronously) until a new connection arrives
    /// or the listener is closed.
    async fn accept(&self) -> Result<Self::Socket, NetError>;

    /// Return the local address this listener is bound to.
    fn local_addr(&self) -> Address;
}
