use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use hf3fs_types::Status;

/// Trait implemented by RPC service handlers.
///
/// Each service is identified by a numeric `service_id` and exposes methods
/// identified by `method_id`. The handler receives a raw request payload and
/// returns either a raw response payload or a `Status` error.
#[async_trait]
pub trait ServiceHandler: Send + Sync {
    /// Unique numeric identifier for this service.
    fn service_id(&self) -> u16;

    /// Human-readable name (used for logging / diagnostics).
    fn service_name(&self) -> &str;

    /// Dispatch a method call.
    async fn handle(&self, method_id: u16, request: Bytes) -> Result<Bytes, Status>;
}

/// Registry mapping service IDs to their handlers.
pub struct ServiceRegistry {
    services: DashMap<u16, Box<dyn ServiceHandler>>,
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self {
            services: DashMap::new(),
        }
    }

    /// Register a service handler. Replaces any previously registered handler
    /// with the same service ID.
    pub fn register(&self, service: Box<dyn ServiceHandler>) {
        let id = service.service_id();
        self.services.insert(id, service);
    }

    /// Look up a service by its ID.
    pub fn get(
        &self,
        service_id: u16,
    ) -> Option<dashmap::mapref::one::Ref<'_, u16, Box<dyn ServiceHandler>>> {
        self.services.get(&service_id)
    }

    /// Remove a service by its ID.
    pub fn unregister(&self, service_id: u16) -> bool {
        self.services.remove(&service_id).is_some()
    }

    /// Return the number of registered services.
    pub fn len(&self) -> usize {
        self.services.len()
    }

    /// Return whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
    }
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hf3fs_types::status_code::StatusCode;

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

    struct FailService;

    #[async_trait]
    impl ServiceHandler for FailService {
        fn service_id(&self) -> u16 {
            2
        }
        fn service_name(&self) -> &str {
            "fail"
        }
        async fn handle(&self, _method_id: u16, _request: Bytes) -> Result<Bytes, Status> {
            Err(Status::new(StatusCode::UNKNOWN))
        }
    }

    #[test]
    fn test_register_and_lookup() {
        let registry = ServiceRegistry::new();
        registry.register(Box::new(EchoService));
        registry.register(Box::new(FailService));

        assert!(registry.get(1).is_some());
        assert_eq!(registry.get(1).unwrap().service_name(), "echo");
        assert!(registry.get(2).is_some());
        assert!(registry.get(99).is_none());
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn test_unregister() {
        let registry = ServiceRegistry::new();
        registry.register(Box::new(EchoService));
        assert!(registry.get(1).is_some());

        assert!(registry.unregister(1));
        assert!(registry.get(1).is_none());
        assert!(!registry.unregister(1)); // already removed
    }

    #[test]
    fn test_register_replaces() {
        let registry = ServiceRegistry::new();
        registry.register(Box::new(EchoService));
        assert_eq!(registry.get(1).unwrap().service_name(), "echo");

        // Register a new service with the same ID.
        struct AnotherService;

        #[async_trait]
        impl ServiceHandler for AnotherService {
            fn service_id(&self) -> u16 {
                1
            }
            fn service_name(&self) -> &str {
                "another"
            }
            async fn handle(&self, _method_id: u16, _request: Bytes) -> Result<Bytes, Status> {
                Ok(Bytes::new())
            }
        }

        registry.register(Box::new(AnotherService));
        assert_eq!(registry.get(1).unwrap().service_name(), "another");
        assert_eq!(registry.len(), 1);
    }

    #[tokio::test]
    async fn test_echo_handler() {
        let handler = EchoService;
        let req = Bytes::from_static(b"hello");
        let resp = handler.handle(0, req.clone()).await.unwrap();
        assert_eq!(resp, req);
    }

    #[tokio::test]
    async fn test_fail_handler() {
        let handler = FailService;
        let result = handler.handle(0, Bytes::new()).await;
        assert!(result.is_err());
    }
}
