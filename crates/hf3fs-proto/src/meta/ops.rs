//! Meta service operation request/response types.
//!
//! Based on 3FS/src/fbs/meta/Service.h

use hf3fs_serde::{WireDeserialize, WireSerialize};
use serde::{Deserialize, Serialize};

use super::types::*;

// ---- Auth ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct AuthReq {
    pub base: ReqBase,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct AuthRsp {
    pub user: UserInfo,
}

// ---- StatFs ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct StatFsReq {
    pub base: ReqBase,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct StatFsRsp {
    pub capacity: u64,
    pub used: u64,
    pub free: u64,
}

// ---- Stat ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct StatReq {
    pub base: ReqBase,
    pub path: PathAt,
    pub flags: AtFlags,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct StatRsp {
    pub stat: Inode,
}

// ---- BatchStat ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct BatchStatReq {
    pub base: ReqBase,
    pub inode_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct BatchStatRsp {
    pub inodes: Vec<Option<Inode>>,
}

// ---- BatchStatByPath ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct BatchStatByPathReq {
    pub base: ReqBase,
    pub paths: Vec<PathAt>,
    pub flags: AtFlags,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct BatchStatByPathRsp {
    pub inodes: Vec<Option<Inode>>,
}

// ---- Create ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct CreateReq {
    pub base: ReqBase,
    pub path: PathAt,
    pub session: Option<SessionInfo>,
    pub flags: OpenFlags,
    pub perm: Permission,
    pub layout: Option<Layout>,
    pub remove_chunks_batch_size: u32,
    pub dyn_stripe: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct CreateRsp {
    pub stat: Inode,
    pub need_truncate: bool,
}

// ---- Mkdirs ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct MkdirsReq {
    pub base: ReqBase,
    pub path: PathAt,
    pub perm: Permission,
    pub recursive: bool,
    pub layout: Option<Layout>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct MkdirsRsp {
    pub stat: Inode,
}

// ---- Symlink ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct SymlinkReq {
    pub base: ReqBase,
    pub path: PathAt,
    pub target: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct SymlinkRsp {
    pub stat: Inode,
}

// ---- HardLink ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct HardLinkReq {
    pub base: ReqBase,
    pub old_path: PathAt,
    pub new_path: PathAt,
    pub flags: AtFlags,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct HardLinkRsp {
    pub stat: Inode,
}

// ---- Remove ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct RemoveReq {
    pub base: ReqBase,
    pub path: PathAt,
    pub at_flags: AtFlags,
    pub recursive: bool,
    pub check_type: bool,
    pub inode_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct RemoveRsp {}

// ---- Open ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct OpenReq {
    pub base: ReqBase,
    pub path: PathAt,
    pub session: Option<SessionInfo>,
    pub flags: OpenFlags,
    pub remove_chunks_batch_size: u32,
    pub dyn_stripe: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct OpenRsp {
    pub stat: Inode,
    pub need_truncate: bool,
}

// ---- Sync ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct SyncReq {
    pub base: ReqBase,
    pub inode: u64,
    pub update_length: bool,
    pub atime: Option<i64>,
    pub mtime: Option<i64>,
    pub truncated: bool,
    pub length_hint: Option<VersionedLength>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct SyncRsp {
    pub stat: Inode,
}

// ---- Close ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct CloseReq {
    pub base: ReqBase,
    pub inode: u64,
    pub session: Option<SessionInfo>,
    pub update_length: bool,
    pub atime: Option<i64>,
    pub mtime: Option<i64>,
    pub length_hint: Option<VersionedLength>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct CloseRsp {
    pub stat: Inode,
}

// ---- Rename ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct RenameReq {
    pub base: ReqBase,
    pub src: PathAt,
    pub dest: PathAt,
    pub move_to_trash: bool,
    pub inode_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct RenameRsp {
    pub stat: Option<Inode>,
}

// ---- List ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct ListReq {
    pub base: ReqBase,
    pub path: PathAt,
    pub prev: String,
    pub limit: i32,
    pub status: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct ListRsp {
    pub entries: Vec<DirEntry>,
    pub inodes: Vec<Inode>,
    pub more: bool,
}

// ---- Truncate ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct TruncateReq {
    pub base: ReqBase,
    pub inode: u64,
    pub length: u64,
    pub remove_chunks_batch_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct TruncateRsp {
    pub chunks_removed: u32,
    pub stat: Inode,
    pub finished: bool,
}

// ---- GetRealPath ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct GetRealPathReq {
    pub base: ReqBase,
    pub path: PathAt,
    pub absolute: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct GetRealPathRsp {
    pub path: String,
}

// ---- SetAttr ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct SetAttrReq {
    pub base: ReqBase,
    pub path: PathAt,
    pub flags: AtFlags,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub perm: Option<u32>,
    pub atime: Option<i64>,
    pub mtime: Option<i64>,
    pub layout: Option<Layout>,
    pub iflags: Option<u32>,
    pub dyn_stripe: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct SetAttrRsp {
    pub stat: Inode,
}

// ---- PruneSession ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct PruneSessionReq {
    pub base: ReqBase,
    pub client: ClientId,
    pub sessions: Vec<ClientId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct PruneSessionRsp {}

// ---- DropUserCache ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct DropUserCacheReq {
    pub base: ReqBase,
    pub uid: Option<u32>,
    pub drop_all: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct DropUserCacheRsp {}

// ---- LockDirectory ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct LockDirectoryReq {
    pub base: ReqBase,
    pub inode: u64,
    pub action: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct LockDirectoryRsp {}

// ---- TestRpc ----

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct TestRpcReq {
    pub base: ReqBase,
    pub path: PathAt,
    pub flags: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct TestRpcRsp {
    pub stat: Inode,
}

#[cfg(test)]
mod tests {
    use super::*;
    use hf3fs_serde::{WireDeserialize, WireSerialize};

    fn roundtrip<T: WireSerialize + WireDeserialize + std::fmt::Debug + PartialEq>(val: &T) -> T {
        let mut buf = Vec::new();
        val.wire_serialize(&mut buf).unwrap();
        let mut offset = 0;
        let result = T::wire_deserialize(&buf, &mut offset).unwrap();
        assert_eq!(offset, buf.len());
        result
    }

    #[test]
    fn test_stat_req_roundtrip() {
        let req = StatReq {
            base: ReqBase::default(),
            path: PathAt {
                parent: 0,
                path: Some("/foo".to_string()),
            },
            flags: AtFlags(0),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn test_stat_rsp_roundtrip() {
        let rsp = StatRsp {
            stat: Inode {
                id: 1,
                inode_type: 0,
                permission: 0o644,
                uid: 1000,
                gid: 100,
                nlink: 1,
                length: 1024,
                atime_ns: 100,
                mtime_ns: 200,
                ctime_ns: 300,
                iflags: 0,
                layout: None,
                symlink_target: None,
            },
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_create_req_roundtrip() {
        let req = CreateReq {
            base: ReqBase::default(),
            path: PathAt {
                parent: 0,
                path: Some("new_file.txt".to_string()),
            },
            session: Some(SessionInfo {
                client: ClientId { high: 1, low: 2 },
                session: ClientId { high: 3, low: 4 },
            }),
            flags: OpenFlags(2),
            perm: Permission(0o644),
            layout: None,
            remove_chunks_batch_size: 32,
            dyn_stripe: false,
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn test_list_rsp_roundtrip() {
        let rsp = ListRsp {
            entries: vec![
                DirEntry {
                    name: "a.txt".to_string(),
                    inode_id: 100,
                    inode_type: 0,
                },
                DirEntry {
                    name: "subdir".to_string(),
                    inode_id: 200,
                    inode_type: 1,
                },
            ],
            inodes: vec![],
            more: true,
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_rename_req_roundtrip() {
        let req = RenameReq {
            base: ReqBase::default(),
            src: PathAt {
                parent: 0,
                path: Some("old.txt".to_string()),
            },
            dest: PathAt {
                parent: 0,
                path: Some("new.txt".to_string()),
            },
            move_to_trash: false,
            inode_id: None,
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn test_set_attr_req_roundtrip() {
        let req = SetAttrReq {
            base: ReqBase::default(),
            path: PathAt {
                parent: 0,
                path: Some("/test".to_string()),
            },
            flags: AtFlags(0),
            uid: Some(1000),
            gid: None,
            perm: Some(0o755),
            atime: None,
            mtime: None,
            layout: None,
            iflags: None,
            dyn_stripe: 0,
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn test_truncate_roundtrip() {
        let req = TruncateReq {
            base: ReqBase::default(),
            inode: 42,
            length: 1024,
            remove_chunks_batch_size: 32,
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = TruncateRsp {
            chunks_removed: 5,
            stat: Inode::default(),
            finished: true,
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_statfs_roundtrip() {
        let rsp = StatFsRsp {
            capacity: 1_000_000_000,
            used: 500_000_000,
            free: 500_000_000,
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_open_req_roundtrip() {
        let req = OpenReq {
            base: ReqBase::default(),
            path: PathAt {
                parent: 0,
                path: Some("file.txt".to_string()),
            },
            session: None,
            flags: OpenFlags(0),
            remove_chunks_batch_size: 32,
            dyn_stripe: false,
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn test_sync_req_roundtrip() {
        let req = SyncReq {
            base: ReqBase::default(),
            inode: 100,
            update_length: true,
            atime: Some(1000),
            mtime: Some(2000),
            truncated: false,
            length_hint: Some(VersionedLength {
                version: 1,
                length: 4096,
            }),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn test_close_req_roundtrip() {
        let req = CloseReq {
            base: ReqBase::default(),
            inode: 42,
            session: Some(SessionInfo::default()),
            update_length: true,
            atime: None,
            mtime: Some(5000),
            length_hint: None,
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn test_batch_stat_roundtrip() {
        let req = BatchStatReq {
            base: ReqBase::default(),
            inode_ids: vec![1, 2, 3, 4],
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = BatchStatRsp {
            inodes: vec![Some(Inode::default()), None, Some(Inode::default())],
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }
}
