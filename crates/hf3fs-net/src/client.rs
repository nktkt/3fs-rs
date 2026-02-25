use bytes::{Bytes, BytesMut, BufMut};
use hf3fs_types::Address;

use crate::error::SocketError;
use crate::message::{MessageHeader, MESSAGE_HEADER_SIZE, SERDE_MESSAGE_MAGIC_NUM};
use crate::socket::Socket;
use crate::transport::{AsyncConnector, Transport};

/// An RPC client that sends requests over a `Transport`.
///
/// Requests are framed with a `MessageHeader`. The body layout is:
/// `[service_id: u16 LE][method_id: u16 LE][payload...]`.
pub struct RpcClient<S: Socket> {
    transport: Transport<S>,
}

impl<S: Socket> RpcClient<S> {
    pub fn new(transport: Transport<S>) -> Self {
        Self { transport }
    }

    /// Send an RPC request to `addr` and wait for the response.
    ///
    /// The `connector` is used to establish a new connection if one is not
    /// already cached in the underlying transport.
    pub async fn call(
        &self,
        addr: Address,
        service_id: u16,
        method_id: u16,
        request: Bytes,
        connector: &(dyn AsyncConnector<S> + '_),
    ) -> Result<Bytes, SocketError> {
        let socket = self.transport.get_or_connect(addr, connector).await?;

        // Build the payload: service_id + method_id + request body.
        let payload_len = 4 + request.len(); // 2 bytes service_id + 2 bytes method_id + body
        let header = MessageHeader {
            checksum: SERDE_MESSAGE_MAGIC_NUM as u32,
            size: payload_len as u32,
        };

        let mut buf = BytesMut::with_capacity(MESSAGE_HEADER_SIZE + payload_len);
        buf.extend_from_slice(&header.to_bytes());
        buf.put_u16_le(service_id);
        buf.put_u16_le(method_id);
        buf.extend_from_slice(&request);

        socket.send(buf.freeze()).await?;

        // Read the response.
        let response = socket.recv().await?;
        Ok(response)
    }

    /// Return a reference to the underlying transport.
    pub fn transport(&self) -> &Transport<S> {
        &self.transport
    }
}

impl<S: Socket> Default for RpcClient<S> {
    fn default() -> Self {
        Self::new(Transport::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use hf3fs_types::{Address, AddressType};
    use parking_lot::Mutex;

    /// A mock socket that records what was sent and returns a canned response.
    struct MockSocket {
        peer: Address,
        sent: Mutex<Vec<Bytes>>,
        response: Bytes,
    }

    #[async_trait]
    impl Socket for MockSocket {
        async fn send(&self, data: Bytes) -> Result<(), SocketError> {
            self.sent.lock().push(data);
            Ok(())
        }
        async fn recv(&self) -> Result<Bytes, SocketError> {
            Ok(self.response.clone())
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
        response: Bytes,
    }

    #[async_trait]
    impl AsyncConnector<MockSocket> for MockConnector {
        async fn connect(&self, addr: Address) -> Result<MockSocket, SocketError> {
            Ok(MockSocket {
                peer: addr,
                sent: Mutex::new(Vec::new()),
                response: self.response.clone(),
            })
        }
    }

    #[tokio::test]
    async fn test_rpc_call() {
        let client = RpcClient::<MockSocket>::default();
        let connector = MockConnector {
            response: Bytes::from_static(b"response-data"),
        };
        let addr = Address::from_octets(10, 0, 0, 1, 8080, AddressType::TCP);

        let resp = client
            .call(addr, 1, 2, Bytes::from_static(b"req"), &connector)
            .await
            .unwrap();

        assert_eq!(resp, Bytes::from_static(b"response-data"));
    }
}
