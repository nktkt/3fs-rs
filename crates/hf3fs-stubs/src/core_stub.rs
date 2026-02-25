//! Core service stub trait and mock implementation.

use async_trait::async_trait;
use hf3fs_proto::core::{
    EchoReq, EchoRsp, GetConfigReq, GetConfigRsp, GetLastConfigUpdateRecordReq,
    GetLastConfigUpdateRecordRsp, HotUpdateConfigReq, HotUpdateConfigRsp, RenderConfigReq,
    RenderConfigRsp, ShutdownReq, ShutdownRsp,
};
use hf3fs_types::Result;
use parking_lot::Mutex;
use std::sync::Arc;

/// Client-side stub for calling the core service.
///
/// Every 3FS service embeds the core service, which provides configuration
/// management, health-check (echo), and graceful shutdown RPCs.
#[async_trait]
pub trait ICoreServiceStub: Send + Sync {
    async fn echo(&self, req: EchoReq) -> Result<EchoRsp>;
    async fn get_config(&self, req: GetConfigReq) -> Result<GetConfigRsp>;
    async fn render_config(&self, req: RenderConfigReq) -> Result<RenderConfigRsp>;
    async fn hot_update_config(&self, req: HotUpdateConfigReq) -> Result<HotUpdateConfigRsp>;
    async fn get_last_config_update_record(
        &self,
        req: GetLastConfigUpdateRecordReq,
    ) -> Result<GetLastConfigUpdateRecordRsp>;
    async fn shutdown(&self, req: ShutdownReq) -> Result<ShutdownRsp>;
}

// ---------------------------------------------------------------------------
// Mock implementation
// ---------------------------------------------------------------------------

type Handler<Req, Rsp> = Box<dyn Fn(Req) -> Result<Rsp> + Send + Sync>;

/// A configurable mock for [`ICoreServiceStub`].
///
/// Each RPC method can be overridden with a closure. If no handler is
/// installed the mock returns a default (success) response.
pub struct MockCoreServiceStub {
    pub echo_handler: Mutex<Option<Handler<EchoReq, EchoRsp>>>,
    pub get_config_handler: Mutex<Option<Handler<GetConfigReq, GetConfigRsp>>>,
    pub render_config_handler: Mutex<Option<Handler<RenderConfigReq, RenderConfigRsp>>>,
    pub hot_update_config_handler: Mutex<Option<Handler<HotUpdateConfigReq, HotUpdateConfigRsp>>>,
    pub get_last_config_update_record_handler:
        Mutex<Option<Handler<GetLastConfigUpdateRecordReq, GetLastConfigUpdateRecordRsp>>>,
    pub shutdown_handler: Mutex<Option<Handler<ShutdownReq, ShutdownRsp>>>,
}

impl MockCoreServiceStub {
    pub fn new() -> Self {
        Self {
            echo_handler: Mutex::new(None),
            get_config_handler: Mutex::new(None),
            render_config_handler: Mutex::new(None),
            hot_update_config_handler: Mutex::new(None),
            get_last_config_update_record_handler: Mutex::new(None),
            shutdown_handler: Mutex::new(None),
        }
    }

    /// Wrap in an `Arc` for convenient sharing.
    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }

    pub fn on_echo(&self, f: impl Fn(EchoReq) -> Result<EchoRsp> + Send + Sync + 'static) {
        *self.echo_handler.lock() = Some(Box::new(f));
    }

    pub fn on_get_config(
        &self,
        f: impl Fn(GetConfigReq) -> Result<GetConfigRsp> + Send + Sync + 'static,
    ) {
        *self.get_config_handler.lock() = Some(Box::new(f));
    }

    pub fn on_render_config(
        &self,
        f: impl Fn(RenderConfigReq) -> Result<RenderConfigRsp> + Send + Sync + 'static,
    ) {
        *self.render_config_handler.lock() = Some(Box::new(f));
    }

    pub fn on_hot_update_config(
        &self,
        f: impl Fn(HotUpdateConfigReq) -> Result<HotUpdateConfigRsp> + Send + Sync + 'static,
    ) {
        *self.hot_update_config_handler.lock() = Some(Box::new(f));
    }

    pub fn on_get_last_config_update_record(
        &self,
        f: impl Fn(GetLastConfigUpdateRecordReq) -> Result<GetLastConfigUpdateRecordRsp>
            + Send
            + Sync
            + 'static,
    ) {
        *self.get_last_config_update_record_handler.lock() = Some(Box::new(f));
    }

    pub fn on_shutdown(
        &self,
        f: impl Fn(ShutdownReq) -> Result<ShutdownRsp> + Send + Sync + 'static,
    ) {
        *self.shutdown_handler.lock() = Some(Box::new(f));
    }
}

impl Default for MockCoreServiceStub {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ICoreServiceStub for MockCoreServiceStub {
    async fn echo(&self, req: EchoReq) -> Result<EchoRsp> {
        let guard = self.echo_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(EchoRsp {
                str: req.str.clone(),
            }),
        }
    }

    async fn get_config(&self, req: GetConfigReq) -> Result<GetConfigRsp> {
        let guard = self.get_config_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(GetConfigRsp {
                config: String::new(),
            }),
        }
    }

    async fn render_config(&self, req: RenderConfigReq) -> Result<RenderConfigRsp> {
        let guard = self.render_config_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(RenderConfigRsp {
                config_after_render: String::new(),
                update_status: 0,
                config_after_update: String::new(),
            }),
        }
    }

    async fn hot_update_config(&self, req: HotUpdateConfigReq) -> Result<HotUpdateConfigRsp> {
        let guard = self.hot_update_config_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(HotUpdateConfigRsp {}),
        }
    }

    async fn get_last_config_update_record(
        &self,
        req: GetLastConfigUpdateRecordReq,
    ) -> Result<GetLastConfigUpdateRecordRsp> {
        let guard = self.get_last_config_update_record_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(GetLastConfigUpdateRecordRsp { record: None }),
        }
    }

    async fn shutdown(&self, req: ShutdownReq) -> Result<ShutdownRsp> {
        let guard = self.shutdown_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(ShutdownRsp {}),
        }
    }
}

/// Blanket implementation: if we have an `Arc<T>` where `T: ICoreServiceStub`,
/// then `Arc<T>` also implements the trait via delegation.
#[async_trait]
impl<T: ICoreServiceStub + ?Sized> ICoreServiceStub for Arc<T> {
    async fn echo(&self, req: EchoReq) -> Result<EchoRsp> {
        (**self).echo(req).await
    }
    async fn get_config(&self, req: GetConfigReq) -> Result<GetConfigRsp> {
        (**self).get_config(req).await
    }
    async fn render_config(&self, req: RenderConfigReq) -> Result<RenderConfigRsp> {
        (**self).render_config(req).await
    }
    async fn hot_update_config(&self, req: HotUpdateConfigReq) -> Result<HotUpdateConfigRsp> {
        (**self).hot_update_config(req).await
    }
    async fn get_last_config_update_record(
        &self,
        req: GetLastConfigUpdateRecordReq,
    ) -> Result<GetLastConfigUpdateRecordRsp> {
        (**self).get_last_config_update_record(req).await
    }
    async fn shutdown(&self, req: ShutdownReq) -> Result<ShutdownRsp> {
        (**self).shutdown(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_echo_default() {
        let mock = MockCoreServiceStub::new();
        let rsp = mock
            .echo(EchoReq {
                str: "hello".into(),
            })
            .await
            .unwrap();
        assert_eq!(rsp.str, "hello");
    }

    #[tokio::test]
    async fn test_mock_echo_custom_handler() {
        let mock = MockCoreServiceStub::new();
        mock.on_echo(|req| {
            Ok(EchoRsp {
                str: format!("echo: {}", req.str),
            })
        });
        let rsp = mock
            .echo(EchoReq {
                str: "world".into(),
            })
            .await
            .unwrap();
        assert_eq!(rsp.str, "echo: world");
    }

    #[tokio::test]
    async fn test_mock_via_arc() {
        let mock = MockCoreServiceStub::new().into_arc();
        let rsp = mock
            .echo(EchoReq {
                str: "arc".into(),
            })
            .await
            .unwrap();
        assert_eq!(rsp.str, "arc");
    }

    #[tokio::test]
    async fn test_mock_shutdown_default() {
        let mock = MockCoreServiceStub::new();
        let rsp = mock.shutdown(ShutdownReq { graceful: true }).await;
        assert!(rsp.is_ok());
    }
}
