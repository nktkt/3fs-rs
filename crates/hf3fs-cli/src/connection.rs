//! Connection helpers and admin environment.
//!
//! Mirrors the C++ `AdminEnv` struct from `3FS/src/client/cli/admin/AdminEnv.h`
//! and the `IEnv` interface from `3FS/src/client/cli/common/IEnv.h`.
//!
//! Provides the `AdminEnv` context that CLI commands use to access cluster services
//! (mgmtd, meta, storage) and track current user/directory state.

use hf3fs_proto::mgmtd::NodeType;
use std::fmt;
use std::path::PathBuf;

/// Global CLI connection options shared across all subcommands.
///
/// These are the top-level flags that configure how the CLI connects to the
/// 3FS cluster, parsed before any subcommand.
#[derive(Debug, Clone, clap::Args)]
pub struct ConnectionOptions {
    /// Path to the admin configuration file.
    #[arg(
        long,
        env = "HF3FS_CONFIG",
        default_value = "~/.hf3fs/admin.toml"
    )]
    pub config: String,

    /// Cluster ID to connect to.
    #[arg(long, env = "HF3FS_CLUSTER_ID")]
    pub cluster_id: Option<String>,

    /// Management daemon address(es), comma-separated.
    #[arg(long, env = "HF3FS_MGMTD_ADDR")]
    pub mgmtd_addr: Option<String>,

    /// Run as superuser (uid=0, gid=0).
    #[arg(long, default_value_t = false)]
    pub as_super: bool,

    /// Connection timeout in seconds.
    #[arg(long, default_value_t = 30)]
    pub timeout_secs: u64,
}

impl ConnectionOptions {
    /// Resolve the config path, expanding `~` to the home directory.
    pub fn resolved_config_path(&self) -> PathBuf {
        let path = &self.config;
        if path.starts_with("~/") {
            if let Ok(home) = std::env::var("HOME") {
                return PathBuf::from(home).join(&path[2..]);
            }
        }
        PathBuf::from(path)
    }
}

/// Admin environment providing access to cluster services.
///
/// Mirrors the C++ `AdminEnv` struct which holds client getters for mgmtd,
/// meta, storage, and core services along with current user/directory state.
///
/// In the Rust implementation, the actual service clients are placeholders
/// (the `hf3fs-client` crate is still a stub). This struct captures the
/// architecture so that commands can be wired up once the client is implemented.
pub struct AdminEnv {
    /// Connection options from the command line.
    pub options: ConnectionOptions,

    /// Current user info (uid, gid).
    pub user_info: UserInfo,

    /// Current working directory inode ID (root = 1).
    pub current_dir_id: u64,

    /// Current working directory path string.
    pub current_dir: String,
}

impl AdminEnv {
    /// Create a new `AdminEnv` from connection options.
    pub fn new(options: ConnectionOptions) -> Self {
        let user_info = if options.as_super {
            UserInfo {
                uid: 0,
                gid: 0,
                token: String::new(),
            }
        } else {
            UserInfo::current_user()
        };

        Self {
            options,
            user_info,
            current_dir_id: ROOT_INODE_ID,
            current_dir: "/".to_string(),
        }
    }

    /// Change the current working directory.
    pub fn set_current_dir(&mut self, id: u64, path: String) {
        self.current_dir_id = id;
        self.current_dir = path;
    }
}

impl fmt::Debug for AdminEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdminEnv")
            .field("user_info", &self.user_info)
            .field("current_dir_id", &self.current_dir_id)
            .field("current_dir", &self.current_dir)
            .finish()
    }
}

/// The root inode ID, matching `meta::InodeId::root()` in C++.
pub const ROOT_INODE_ID: u64 = 1;

/// User identity information for authentication with cluster services.
///
/// Mirrors the C++ `flat::UserInfo` struct.
#[derive(Debug, Clone)]
pub struct UserInfo {
    pub uid: u32,
    pub gid: u32,
    pub token: String,
}

impl UserInfo {
    /// Get the current OS user's info.
    pub fn current_user() -> Self {
        #[cfg(unix)]
        {
            Self {
                uid: unsafe { libc::getuid() },
                gid: unsafe { libc::getgid() },
                token: String::new(),
            }
        }
        #[cfg(not(unix))]
        {
            Self {
                uid: 1000,
                gid: 1000,
                token: String::new(),
            }
        }
    }
}

/// Node type string mapping, mirroring the C++ `typeMappings` used
/// in get-config, set-config, etc.
pub fn parse_node_type(s: &str) -> Option<NodeType> {
    match s.to_uppercase().as_str() {
        "META" => Some(NodeType::Meta),
        "STORAGE" => Some(NodeType::Storage),
        "CLIENT" | "CLIENT_AGENT" | "FUSE" => Some(NodeType::Client),
        "MGMTD" => Some(NodeType::Mgmtd),
        _ => None,
    }
}

/// Get the display name for a node type.
pub fn node_type_name(nt: NodeType) -> &'static str {
    match nt {
        NodeType::Meta => "META",
        NodeType::Storage => "STORAGE",
        NodeType::Client => "CLIENT",
        NodeType::Mgmtd => "MGMTD",
        NodeType::Fuse => "FUSE",
    }
}

/// All valid node type choice strings for CLI help text.
pub const NODE_TYPE_CHOICES: &[&str] = &["META", "STORAGE", "CLIENT", "MGMTD", "FUSE"];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_node_type() {
        assert_eq!(parse_node_type("META"), Some(NodeType::Meta));
        assert_eq!(parse_node_type("meta"), Some(NodeType::Meta));
        assert_eq!(parse_node_type("STORAGE"), Some(NodeType::Storage));
        assert_eq!(parse_node_type("CLIENT"), Some(NodeType::Client));
        assert_eq!(parse_node_type("FUSE"), Some(NodeType::Client));
        assert_eq!(parse_node_type("CLIENT_AGENT"), Some(NodeType::Client));
        assert_eq!(parse_node_type("MGMTD"), Some(NodeType::Mgmtd));
        assert_eq!(parse_node_type("unknown"), None);
    }

    #[test]
    fn test_node_type_name() {
        assert_eq!(node_type_name(NodeType::Meta), "META");
        assert_eq!(node_type_name(NodeType::Storage), "STORAGE");
        assert_eq!(node_type_name(NodeType::Client), "CLIENT");
        assert_eq!(node_type_name(NodeType::Mgmtd), "MGMTD");
    }

    #[test]
    fn test_user_info_current() {
        let info = UserInfo::current_user();
        // Should not panic; values depend on OS.
        let _ = info.uid;
        let _ = info.gid;
    }

    #[test]
    fn test_admin_env_new() {
        let options = ConnectionOptions {
            config: "~/.hf3fs/admin.toml".to_string(),
            cluster_id: None,
            mgmtd_addr: None,
            as_super: false,
            timeout_secs: 30,
        };
        let env = AdminEnv::new(options);
        assert_eq!(env.current_dir, "/");
        assert_eq!(env.current_dir_id, ROOT_INODE_ID);
    }

    #[test]
    fn test_admin_env_super_user() {
        let options = ConnectionOptions {
            config: "test.toml".to_string(),
            cluster_id: None,
            mgmtd_addr: None,
            as_super: true,
            timeout_secs: 30,
        };
        let env = AdminEnv::new(options);
        assert_eq!(env.user_info.uid, 0);
        assert_eq!(env.user_info.gid, 0);
    }

    #[test]
    fn test_resolved_config_path_absolute() {
        let options = ConnectionOptions {
            config: "/etc/hf3fs/admin.toml".to_string(),
            cluster_id: None,
            mgmtd_addr: None,
            as_super: false,
            timeout_secs: 30,
        };
        assert_eq!(
            options.resolved_config_path(),
            PathBuf::from("/etc/hf3fs/admin.toml")
        );
    }

    #[test]
    fn test_set_current_dir() {
        let options = ConnectionOptions {
            config: "test.toml".to_string(),
            cluster_id: None,
            mgmtd_addr: None,
            as_super: false,
            timeout_secs: 30,
        };
        let mut env = AdminEnv::new(options);
        env.set_current_dir(42, "/data/test".to_string());
        assert_eq!(env.current_dir_id, 42);
        assert_eq!(env.current_dir, "/data/test");
    }
}
