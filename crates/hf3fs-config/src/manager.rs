use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::{Config, ConfigError};

/// Manages a configuration with hot-update support.
pub struct ConfigManager<T: Config> {
    config: ArcSwap<T>,
    path: Option<PathBuf>,
}

impl<T: Config> ConfigManager<T> {
    pub fn new(config: T) -> Self {
        Self {
            config: ArcSwap::from_pointee(config),
            path: None,
        }
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)?;
        let value: toml::Value = content.parse()?;
        let config = T::from_toml(&value)?;
        config.validate()?;
        Ok(Self {
            config: ArcSwap::from_pointee(config),
            path: Some(path.to_path_buf()),
        })
    }

    pub fn get(&self) -> arc_swap::Guard<Arc<T>> {
        self.config.load()
    }

    pub fn update(&self, new_config: T) -> Result<(), ConfigError> {
        new_config.validate()?;
        self.config.store(Arc::new(new_config));
        Ok(())
    }

}

impl<T: Config + Clone> ConfigManager<T> {
    pub fn reload(&self) -> Result<(), ConfigError> {
        if let Some(ref path) = self.path {
            let content = std::fs::read_to_string(path)?;
            let value: toml::Value = content.parse()?;
            let new_config = T::from_toml(&value)?;
            new_config.validate()?;

            // Hot update: only update hot_updated fields
            let mut current = (*self.config.load_full()).clone();
            current.hot_update(&new_config);
            self.config.store(Arc::new(current));

            tracing::info!("Config reloaded from {:?}", path);
        }
        Ok(())
    }

    pub fn snapshot(&self) -> T {
        (*self.config.load_full()).clone()
    }
}
