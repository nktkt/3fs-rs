//! Core RPC service for hf3fs.
//!
//! Provides fundamental operations: echo (health check), config management,
//! and shutdown. Based on 3FS/src/fbs/core/service/Rpc.h.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::watch;
use tracing;

use hf3fs_config::{Config, ConfigManager};
use hf3fs_proto::core::*;
use hf3fs_service_derive::{hf3fs_service, method};
use hf3fs_types::status::Status;
use hf3fs_types::status_code::StatusCode;

/// Service trait definition for the Core RPC service (service id = 1).
///
/// The `#[hf3fs_service]` macro generates a `i_core_service_service_meta` module
/// containing `SERVICE_ID`, `SERVICE_NAME`, `MethodId` enum, and `method_name()`.
#[hf3fs_service(id = 1, name = "Core")]
#[async_trait]
pub trait ICoreService: Send + Sync {
    /// Echo back the request string. Used for health checks and connectivity testing.
    #[method(id = 1)]
    async fn echo(&self, req: EchoReq) -> Result<EchoRsp, Status>;

    /// Retrieve the current configuration as a rendered string.
    #[method(id = 2)]
    async fn get_config(&self, req: GetConfigReq) -> Result<GetConfigRsp, Status>;

    /// Render a configuration template, optionally testing an update.
    #[method(id = 3)]
    async fn render_config(&self, req: RenderConfigReq) -> Result<RenderConfigRsp, Status>;

    /// Hot-update the running configuration without restart.
    #[method(id = 4)]
    async fn hot_update_config(&self, req: HotUpdateConfigReq) -> Result<HotUpdateConfigRsp, Status>;

    /// Retrieve the last configuration update record.
    #[method(id = 5)]
    async fn get_last_config_update_record(
        &self,
        req: GetLastConfigUpdateRecordReq,
    ) -> Result<GetLastConfigUpdateRecordRsp, Status>;

    /// Initiate a graceful or immediate shutdown.
    #[method(id = 6)]
    async fn shutdown(&self, req: ShutdownReq) -> Result<ShutdownRsp, Status>;
}

/// Record of the last configuration update, including the raw update string
/// and a timestamp.
#[derive(Debug, Clone)]
pub struct ConfigUpdateRecord {
    pub update: String,
    pub timestamp: std::time::SystemTime,
}

/// Implementation of the Core RPC service.
///
/// Generic over `C: Config + Clone` so it can manage any configuration type
/// used by the host application.
pub struct CoreServiceImpl<C: Config + Clone> {
    config_manager: Arc<ConfigManager<C>>,
    shutdown_tx: watch::Sender<ShutdownSignal>,
    last_update_record: tokio::sync::RwLock<Option<ConfigUpdateRecord>>,
}

/// Describes the type of shutdown requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownSignal {
    /// No shutdown requested.
    None,
    /// Graceful shutdown: finish in-flight requests then exit.
    Graceful,
    /// Immediate shutdown: exit as soon as possible.
    Immediate,
}

impl<C: Config + Clone> CoreServiceImpl<C> {
    /// Create a new `CoreServiceImpl`.
    ///
    /// - `config_manager`: shared config manager for reading/updating config.
    /// - `shutdown_tx`: the sending half of a watch channel; receivers listen
    ///   for shutdown signals.
    pub fn new(
        config_manager: Arc<ConfigManager<C>>,
        shutdown_tx: watch::Sender<ShutdownSignal>,
    ) -> Self {
        Self {
            config_manager,
            shutdown_tx,
            last_update_record: tokio::sync::RwLock::new(None),
        }
    }

    /// Subscribe to shutdown signals.
    pub fn shutdown_receiver(&self) -> watch::Receiver<ShutdownSignal> {
        self.shutdown_tx.subscribe()
    }
}

#[async_trait]
impl<C: Config + Clone + Send + Sync + 'static> ICoreService for CoreServiceImpl<C> {
    async fn echo(&self, req: EchoReq) -> Result<EchoRsp, Status> {
        tracing::debug!("echo: {:?}", req.str);
        Ok(EchoRsp { str: req.str })
    }

    async fn get_config(&self, req: GetConfigReq) -> Result<GetConfigRsp, Status> {
        tracing::debug!("get_config: key={:?}", req.config_key);
        let config = self.config_manager.snapshot();
        let rendered = config.render();

        // If a specific key was requested, try to extract it from the rendered
        // TOML. An empty key means "return the entire config".
        if req.config_key.is_empty() {
            Ok(GetConfigRsp { config: rendered })
        } else {
            // Parse the rendered config as TOML and look up the key.
            let value: toml::Value = rendered.parse().map_err(|_| {
                Status::with_message(StatusCode::CONFIG_PARSE_ERROR, "failed to parse rendered config")
            })?;

            let result = lookup_toml_key(&value, &req.config_key);
            match result {
                Some(v) => Ok(GetConfigRsp {
                    config: toml_value_to_string(&v),
                }),
                None => Err(Status::with_message(
                    StatusCode::CONFIG_KEY_NOT_FOUND,
                    format!("key '{}' not found", req.config_key),
                )),
            }
        }
    }

    async fn render_config(&self, req: RenderConfigReq) -> Result<RenderConfigRsp, Status> {
        tracing::debug!(
            "render_config: template_len={}, test_update={}, is_hot_update={}",
            req.config_template.len(),
            req.test_update,
            req.is_hot_update,
        );

        // Parse the provided template as TOML.
        let parsed_value: toml::Value = req.config_template.parse().map_err(|e: toml::de::Error| {
            Status::with_message(
                StatusCode::CONFIG_PARSE_ERROR,
                format!("failed to parse config template: {}", e),
            )
        })?;

        let new_config = C::from_toml(&parsed_value).map_err(|e| {
            Status::with_message(
                StatusCode::CONFIG_PARSE_ERROR,
                format!("failed to build config from template: {}", e),
            )
        })?;

        let rendered_after = new_config.render();

        let (update_status, config_after_update) = if req.test_update {
            // Simulate the update without actually applying it.
            if req.is_hot_update {
                let mut current = self.config_manager.snapshot();
                current.hot_update(&new_config);
                (0i32, current.render())
            } else {
                match new_config.validate() {
                    Ok(()) => (0i32, rendered_after.clone()),
                    Err(e) => (
                        StatusCode::CONFIG_VALIDATE_FAILED as i32,
                        format!("validation failed: {}", e),
                    ),
                }
            }
        } else {
            (0i32, String::new())
        };

        Ok(RenderConfigRsp {
            config_after_render: rendered_after,
            update_status,
            config_after_update,
        })
    }

    async fn hot_update_config(&self, req: HotUpdateConfigReq) -> Result<HotUpdateConfigRsp, Status> {
        tracing::info!(
            "hot_update_config: update_len={}, render={}",
            req.update.len(),
            req.render,
        );

        let parsed_value: toml::Value = req.update.parse().map_err(|e: toml::de::Error| {
            Status::with_message(
                StatusCode::CONFIG_PARSE_ERROR,
                format!("failed to parse update config: {}", e),
            )
        })?;

        let new_config = C::from_toml(&parsed_value).map_err(|e| {
            Status::with_message(
                StatusCode::CONFIG_PARSE_ERROR,
                format!("failed to build config from update: {}", e),
            )
        })?;

        new_config.validate().map_err(|e| {
            Status::with_message(
                StatusCode::CONFIG_VALIDATE_FAILED,
                format!("config validation failed: {}", e),
            )
        })?;

        // Apply hot update: merge only hot_updated fields into current config.
        let mut current = self.config_manager.snapshot();
        current.hot_update(&new_config);
        self.config_manager.update(current).map_err(|e| {
            Status::with_message(
                StatusCode::CONFIG_UPDATE_FAILED,
                format!("failed to apply config update: {}", e),
            )
        })?;

        // Record the update.
        {
            let mut record = self.last_update_record.write().await;
            *record = Some(ConfigUpdateRecord {
                update: req.update,
                timestamp: std::time::SystemTime::now(),
            });
        }

        tracing::info!("hot_update_config: applied successfully");
        Ok(HotUpdateConfigRsp {})
    }

    async fn get_last_config_update_record(
        &self,
        _req: GetLastConfigUpdateRecordReq,
    ) -> Result<GetLastConfigUpdateRecordRsp, Status> {
        let record = self.last_update_record.read().await;
        let record_str = record.as_ref().map(|r| {
            let elapsed = r
                .timestamp
                .elapsed()
                .map(|d| format!("{:.1}s ago", d.as_secs_f64()))
                .unwrap_or_else(|_| "unknown".to_string());
            format!("update={}, timestamp={}", r.update, elapsed)
        });
        Ok(GetLastConfigUpdateRecordRsp { record: record_str })
    }

    async fn shutdown(&self, req: ShutdownReq) -> Result<ShutdownRsp, Status> {
        let signal = if req.graceful {
            tracing::info!("shutdown: graceful shutdown requested");
            ShutdownSignal::Graceful
        } else {
            tracing::warn!("shutdown: immediate shutdown requested");
            ShutdownSignal::Immediate
        };

        self.shutdown_tx.send(signal).map_err(|_| {
            Status::with_message(
                StatusCode::UNKNOWN,
                "failed to send shutdown signal (no receivers)",
            )
        })?;

        Ok(ShutdownRsp {})
    }
}

/// Look up a dotted key path in a TOML value (e.g., "inner.buffer_size").
fn lookup_toml_key<'a>(value: &'a toml::Value, key: &str) -> Option<&'a toml::Value> {
    let mut current = value;
    for part in key.split('.') {
        current = current.get(part)?;
    }
    Some(current)
}

/// Convert a TOML value to a display string.
fn toml_value_to_string(value: &toml::Value) -> String {
    match value {
        toml::Value::String(s) => s.clone(),
        toml::Value::Integer(i) => i.to_string(),
        toml::Value::Float(f) => f.to_string(),
        toml::Value::Boolean(b) => b.to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hf3fs_config::Config;

    // A minimal test config that implements the Config trait manually
    // so we don't need the derive macro in tests.
    #[derive(Debug, Clone)]
    struct TestConfig {
        port: u16,
        host: String,
        workers: i32,
    }

    impl Default for TestConfig {
        fn default() -> Self {
            Self {
                port: 8080,
                host: "localhost".to_string(),
                workers: 4,
            }
        }
    }

    impl Config for TestConfig {
        fn from_toml(value: &toml::Value) -> Result<Self, hf3fs_config::ConfigError> {
            let mut cfg = Self::default();
            if let Some(v) = value.get("port") {
                cfg.port = v
                    .as_integer()
                    .ok_or_else(|| hf3fs_config::ConfigError::TypeMismatch {
                        field: "port".into(),
                        expected: "integer".into(),
                    })? as u16;
            }
            if let Some(v) = value.get("host") {
                cfg.host = v
                    .as_str()
                    .ok_or_else(|| hf3fs_config::ConfigError::TypeMismatch {
                        field: "host".into(),
                        expected: "string".into(),
                    })?
                    .to_string();
            }
            if let Some(v) = value.get("workers") {
                cfg.workers = v
                    .as_integer()
                    .ok_or_else(|| hf3fs_config::ConfigError::TypeMismatch {
                        field: "workers".into(),
                        expected: "integer".into(),
                    })? as i32;
            }
            Ok(cfg)
        }

        fn hot_update(&mut self, other: &Self) {
            // Only workers is hot-updatable.
            self.workers = other.workers;
        }

        fn render(&self) -> String {
            format!(
                "port = {}\nhost = \"{}\"\nworkers = {}",
                self.port, self.host, self.workers
            )
        }

        fn validate(&self) -> Result<(), hf3fs_config::ConfigError> {
            if self.port == 0 {
                return Err(hf3fs_config::ConfigError::OutOfRange {
                    field: "port".into(),
                    value: "0".into(),
                    min: Some("1".into()),
                    max: Some("65535".into()),
                });
            }
            if self.workers < 1 || self.workers > 64 {
                return Err(hf3fs_config::ConfigError::OutOfRange {
                    field: "workers".into(),
                    value: self.workers.to_string(),
                    min: Some("1".into()),
                    max: Some("64".into()),
                });
            }
            Ok(())
        }
    }

    fn make_service() -> (
        CoreServiceImpl<TestConfig>,
        watch::Receiver<ShutdownSignal>,
    ) {
        let config = TestConfig::default();
        let mgr = Arc::new(ConfigManager::new(config));
        let (tx, rx) = watch::channel(ShutdownSignal::None);
        let svc = CoreServiceImpl::new(mgr, tx);
        (svc, rx)
    }

    // ---- echo ----

    #[tokio::test]
    async fn test_echo() {
        let (svc, _rx) = make_service();
        let rsp = svc
            .echo(EchoReq {
                str: "hello".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(rsp.str, "hello");
    }

    #[tokio::test]
    async fn test_echo_empty() {
        let (svc, _rx) = make_service();
        let rsp = svc
            .echo(EchoReq {
                str: String::new(),
            })
            .await
            .unwrap();
        assert_eq!(rsp.str, "");
    }

    #[tokio::test]
    async fn test_echo_unicode() {
        let (svc, _rx) = make_service();
        let rsp = svc
            .echo(EchoReq {
                str: "hello world".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(rsp.str, "hello world");
    }

    // ---- get_config ----

    #[tokio::test]
    async fn test_get_config_full() {
        let (svc, _rx) = make_service();
        let rsp = svc
            .get_config(GetConfigReq {
                config_key: String::new(),
            })
            .await
            .unwrap();
        assert!(rsp.config.contains("port = 8080"), "got: {}", rsp.config);
        assert!(
            rsp.config.contains("host = \"localhost\""),
            "got: {}",
            rsp.config
        );
        assert!(rsp.config.contains("workers = 4"), "got: {}", rsp.config);
    }

    #[tokio::test]
    async fn test_get_config_specific_key() {
        let (svc, _rx) = make_service();
        let rsp = svc
            .get_config(GetConfigReq {
                config_key: "port".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(rsp.config, "8080");
    }

    #[tokio::test]
    async fn test_get_config_missing_key() {
        let (svc, _rx) = make_service();
        let result = svc
            .get_config(GetConfigReq {
                config_key: "nonexistent".to_string(),
            })
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), StatusCode::CONFIG_KEY_NOT_FOUND);
    }

    // ---- render_config ----

    #[tokio::test]
    async fn test_render_config_basic() {
        let (svc, _rx) = make_service();
        let template = "port = 9090\nhost = \"example.com\"\nworkers = 8";
        let rsp = svc
            .render_config(RenderConfigReq {
                config_template: template.to_string(),
                test_update: false,
                is_hot_update: false,
            })
            .await
            .unwrap();
        assert!(
            rsp.config_after_render.contains("port = 9090"),
            "got: {}",
            rsp.config_after_render
        );
        assert!(
            rsp.config_after_render.contains("workers = 8"),
            "got: {}",
            rsp.config_after_render
        );
    }

    #[tokio::test]
    async fn test_render_config_test_hot_update() {
        let (svc, _rx) = make_service();
        // Template changes workers (hot-updatable) and port (not hot-updatable)
        let template = "port = 9090\nworkers = 16";
        let rsp = svc
            .render_config(RenderConfigReq {
                config_template: template.to_string(),
                test_update: true,
                is_hot_update: true,
            })
            .await
            .unwrap();
        assert_eq!(rsp.update_status, 0);
        // After hot update simulation: workers changed, port stays at 8080
        assert!(
            rsp.config_after_update.contains("workers = 16"),
            "got: {}",
            rsp.config_after_update
        );
        assert!(
            rsp.config_after_update.contains("port = 8080"),
            "got: {}",
            rsp.config_after_update
        );
    }

    #[tokio::test]
    async fn test_render_config_invalid_template() {
        let (svc, _rx) = make_service();
        let result = svc
            .render_config(RenderConfigReq {
                config_template: "not valid toml {{{}".to_string(),
                test_update: false,
                is_hot_update: false,
            })
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), StatusCode::CONFIG_PARSE_ERROR);
    }

    // ---- hot_update_config ----

    #[tokio::test]
    async fn test_hot_update_config() {
        let config = TestConfig::default();
        let mgr = Arc::new(ConfigManager::new(config));
        let (tx, _rx) = watch::channel(ShutdownSignal::None);
        let svc = CoreServiceImpl::new(mgr.clone(), tx);

        // Verify initial state
        assert_eq!(mgr.snapshot().workers, 4);
        assert_eq!(mgr.snapshot().port, 8080);

        // Hot update: change workers and port
        let update = "port = 9090\nworkers = 16";
        svc.hot_update_config(HotUpdateConfigReq {
            update: update.to_string(),
            render: false,
        })
        .await
        .unwrap();

        // workers is hot-updatable, port is not
        let snap = mgr.snapshot();
        assert_eq!(snap.workers, 16);
        assert_eq!(snap.port, 8080); // port unchanged - not hot-updatable
    }

    #[tokio::test]
    async fn test_hot_update_config_invalid() {
        let (svc, _rx) = make_service();
        let result = svc
            .hot_update_config(HotUpdateConfigReq {
                update: "not valid toml".to_string(),
                render: false,
            })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_hot_update_config_validation_failure() {
        let (svc, _rx) = make_service();
        // workers = 100 exceeds max of 64
        let result = svc
            .hot_update_config(HotUpdateConfigReq {
                update: "workers = 100".to_string(),
                render: false,
            })
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), StatusCode::CONFIG_VALIDATE_FAILED);
    }

    // ---- get_last_config_update_record ----

    #[tokio::test]
    async fn test_get_last_config_update_record_none() {
        let (svc, _rx) = make_service();
        let rsp = svc
            .get_last_config_update_record(GetLastConfigUpdateRecordReq {})
            .await
            .unwrap();
        assert!(rsp.record.is_none());
    }

    #[tokio::test]
    async fn test_get_last_config_update_record_after_update() {
        let (svc, _rx) = make_service();

        // Do a hot update first.
        svc.hot_update_config(HotUpdateConfigReq {
            update: "workers = 8".to_string(),
            render: false,
        })
        .await
        .unwrap();

        let rsp = svc
            .get_last_config_update_record(GetLastConfigUpdateRecordReq {})
            .await
            .unwrap();
        assert!(rsp.record.is_some());
        let record = rsp.record.unwrap();
        assert!(
            record.contains("workers = 8"),
            "got: {}",
            record
        );
    }

    // ---- shutdown ----

    #[tokio::test]
    async fn test_shutdown_graceful() {
        let (svc, mut rx) = make_service();

        assert_eq!(*rx.borrow(), ShutdownSignal::None);

        svc.shutdown(ShutdownReq { graceful: true })
            .await
            .unwrap();

        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), ShutdownSignal::Graceful);
    }

    #[tokio::test]
    async fn test_shutdown_immediate() {
        let (svc, mut rx) = make_service();

        assert_eq!(*rx.borrow(), ShutdownSignal::None);

        svc.shutdown(ShutdownReq { graceful: false })
            .await
            .unwrap();

        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), ShutdownSignal::Immediate);
    }

    // ---- service metadata ----

    #[test]
    fn test_service_meta_constants() {
        use i_core_service_service_meta::*;

        assert_eq!(SERVICE_ID, 1);
        assert_eq!(SERVICE_NAME, "Core");
    }

    #[test]
    fn test_service_meta_method_ids() {
        use i_core_service_service_meta::*;

        assert_eq!(MethodId::Echo.as_u16(), 1);
        assert_eq!(MethodId::GetConfig.as_u16(), 2);
        assert_eq!(MethodId::RenderConfig.as_u16(), 3);
        assert_eq!(MethodId::HotUpdateConfig.as_u16(), 4);
        assert_eq!(MethodId::GetLastConfigUpdateRecord.as_u16(), 5);
        assert_eq!(MethodId::Shutdown.as_u16(), 6);
    }

    #[test]
    fn test_service_meta_method_from_u16() {
        use i_core_service_service_meta::*;

        assert_eq!(MethodId::from_u16(1), Some(MethodId::Echo));
        assert_eq!(MethodId::from_u16(2), Some(MethodId::GetConfig));
        assert_eq!(MethodId::from_u16(3), Some(MethodId::RenderConfig));
        assert_eq!(MethodId::from_u16(4), Some(MethodId::HotUpdateConfig));
        assert_eq!(
            MethodId::from_u16(5),
            Some(MethodId::GetLastConfigUpdateRecord)
        );
        assert_eq!(MethodId::from_u16(6), Some(MethodId::Shutdown));
        assert_eq!(MethodId::from_u16(0), None);
        assert_eq!(MethodId::from_u16(7), None);
    }

    #[test]
    fn test_service_meta_method_name() {
        use i_core_service_service_meta::*;

        assert_eq!(method_name(1), Some("echo"));
        assert_eq!(method_name(2), Some("get_config"));
        assert_eq!(method_name(3), Some("render_config"));
        assert_eq!(method_name(4), Some("hot_update_config"));
        assert_eq!(method_name(5), Some("get_last_config_update_record"));
        assert_eq!(method_name(6), Some("shutdown"));
        assert_eq!(method_name(7), None);
    }

    // ---- integration-style tests ----

    #[tokio::test]
    async fn test_full_config_lifecycle() {
        let config = TestConfig::default();
        let mgr = Arc::new(ConfigManager::new(config));
        let (tx, _rx) = watch::channel(ShutdownSignal::None);
        let svc = CoreServiceImpl::new(mgr.clone(), tx);

        // 1. Get initial config
        let rsp = svc
            .get_config(GetConfigReq {
                config_key: String::new(),
            })
            .await
            .unwrap();
        assert!(rsp.config.contains("workers = 4"));

        // 2. No update record yet
        let rsp = svc
            .get_last_config_update_record(GetLastConfigUpdateRecordReq {})
            .await
            .unwrap();
        assert!(rsp.record.is_none());

        // 3. Render a potential update
        let rsp = svc
            .render_config(RenderConfigReq {
                config_template: "workers = 32".to_string(),
                test_update: true,
                is_hot_update: true,
            })
            .await
            .unwrap();
        assert_eq!(rsp.update_status, 0);
        assert!(rsp.config_after_update.contains("workers = 32"));

        // Config should not have changed yet (render is read-only)
        assert_eq!(mgr.snapshot().workers, 4);

        // 4. Apply the hot update
        svc.hot_update_config(HotUpdateConfigReq {
            update: "workers = 32".to_string(),
            render: false,
        })
        .await
        .unwrap();
        assert_eq!(mgr.snapshot().workers, 32);

        // 5. Verify the update was recorded
        let rsp = svc
            .get_last_config_update_record(GetLastConfigUpdateRecordReq {})
            .await
            .unwrap();
        assert!(rsp.record.is_some());

        // 6. Get updated config
        let rsp = svc
            .get_config(GetConfigReq {
                config_key: "workers".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(rsp.config, "32");
    }

    #[tokio::test]
    async fn test_shutdown_signal_receiver() {
        let config = TestConfig::default();
        let mgr = Arc::new(ConfigManager::new(config));
        let (tx, _rx) = watch::channel(ShutdownSignal::None);
        let svc = CoreServiceImpl::new(mgr, tx);

        let mut recv = svc.shutdown_receiver();
        assert_eq!(*recv.borrow(), ShutdownSignal::None);

        svc.shutdown(ShutdownReq { graceful: true })
            .await
            .unwrap();

        recv.changed().await.unwrap();
        assert_eq!(*recv.borrow(), ShutdownSignal::Graceful);
    }
}
