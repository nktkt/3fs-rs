//! Meta service common types.
//!
//! Based on 3FS/src/fbs/meta/Common.h

use hf3fs_serde::{WireDeserialize, WireSerialize};
// hf3fs_types re-exports available if needed.
use serde::{Deserialize, Serialize};

/// Access type flags (bitmask).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessType(pub u8);

impl AccessType {
    pub const EXEC: Self = Self(1);
    pub const WRITE: Self = Self(2);
    pub const READ: Self = Self(4);
}

impl WireSerialize for AccessType {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        self.0.wire_serialize(buf)
    }
}

impl WireDeserialize for AccessType {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        Ok(Self(u8::wire_deserialize(buf, offset)?))
    }
}

/// Open flags (mirrors POSIX O_* flags).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct OpenFlags(pub i32);

impl WireSerialize for OpenFlags {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        self.0.wire_serialize(buf)
    }
}

impl WireDeserialize for OpenFlags {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        Ok(Self(i32::wire_deserialize(buf, offset)?))
    }
}

/// Flags for *at() system calls (AT_SYMLINK_NOFOLLOW etc).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AtFlags(pub i32);

impl WireSerialize for AtFlags {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        self.0.wire_serialize(buf)
    }
}

impl WireDeserialize for AtFlags {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        Ok(Self(i32::wire_deserialize(buf, offset)?))
    }
}

/// Permission bits (e.g. 0o755).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Permission(pub u32);

impl WireSerialize for Permission {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        self.0.wire_serialize(buf)
    }
}

impl WireDeserialize for Permission {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        Ok(Self(u32::wire_deserialize(buf, offset)?))
    }
}

/// Inode flags (FS_IMMUTABLE_FL etc).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct IFlags(pub u32);

impl WireSerialize for IFlags {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        self.0.wire_serialize(buf)
    }
}

impl WireDeserialize for IFlags {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        Ok(Self(u32::wire_deserialize(buf, offset)?))
    }
}

/// Inode type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum InodeType {
    File = 0,
    Directory = 1,
    Symlink = 2,
}

impl TryFrom<u8> for InodeType {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(Self::File),
            1 => Ok(Self::Directory),
            2 => Ok(Self::Symlink),
            _ => Err(()),
        }
    }
}

impl WireSerialize for InodeType {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        (*self as u8).wire_serialize(buf)
    }
}

impl WireDeserialize for InodeType {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        let v = u8::wire_deserialize(buf, offset)?;
        Self::try_from(v).map_err(|_| hf3fs_serde::WireError::InvalidEnumVariant {
            enum_name: "InodeType",
            value: v as u64,
        })
    }
}

/// User info for authentication/authorization.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct UserInfo {
    pub uid: u32,
    pub gid: u32,
    pub gids: Vec<u32>,
}

/// Client identifier (UUID represented as pair of u64).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct ClientId {
    pub high: u64,
    pub low: u64,
}

/// Session info for file operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct SessionInfo {
    pub client: ClientId,
    pub session: ClientId,
}

/// Path relative to a parent inode.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct PathAt {
    pub parent: u64,
    pub path: Option<String>,
}

/// Storage layout for a file or directory.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct Layout {
    pub chain_id: u64,
    pub stripe_size: u32,
    pub num_stripes: u32,
}

/// Versioned length used in sync operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct VersionedLength {
    pub version: u64,
    pub length: u64,
}

/// An inode representation.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct Inode {
    pub id: u64,
    pub inode_type: u8,
    pub permission: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub length: u64,
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub iflags: u32,
    pub layout: Option<Layout>,
    pub symlink_target: Option<String>,
}

/// A directory entry.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct DirEntry {
    pub name: String,
    pub inode_id: u64,
    pub inode_type: u8,
}

/// Lock action for directory locking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum LockAction {
    TryLock = 0,
    PreemptLock = 1,
    UnLock = 2,
    Clear = 3,
}

impl TryFrom<u8> for LockAction {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(Self::TryLock),
            1 => Ok(Self::PreemptLock),
            2 => Ok(Self::UnLock),
            3 => Ok(Self::Clear),
            _ => Err(()),
        }
    }
}

impl WireSerialize for LockAction {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        (*self as u8).wire_serialize(buf)
    }
}

impl WireDeserialize for LockAction {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        let v = u8::wire_deserialize(buf, offset)?;
        Self::try_from(v).map_err(|_| hf3fs_serde::WireError::InvalidEnumVariant {
            enum_name: "LockAction",
            value: v as u64,
        })
    }
}

/// Request base fields common to all meta requests.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct ReqBase {
    pub user: UserInfo,
    pub client: ClientId,
    pub forward: u32,
    pub uuid: ClientId,
}

/// Response base (currently empty placeholder).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct RspBase {}

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
    fn test_inode_type_roundtrip() {
        assert_eq!(roundtrip(&InodeType::File), InodeType::File);
        assert_eq!(roundtrip(&InodeType::Directory), InodeType::Directory);
        assert_eq!(roundtrip(&InodeType::Symlink), InodeType::Symlink);
    }

    #[test]
    fn test_user_info_roundtrip() {
        let u = UserInfo {
            uid: 1000,
            gid: 100,
            gids: vec![100, 200, 300],
        };
        assert_eq!(roundtrip(&u), u);
    }

    #[test]
    fn test_path_at_roundtrip() {
        let p = PathAt {
            parent: 42,
            path: Some("/foo/bar".to_string()),
        };
        assert_eq!(roundtrip(&p), p);

        let p_none = PathAt {
            parent: 0,
            path: None,
        };
        assert_eq!(roundtrip(&p_none), p_none);
    }

    #[test]
    fn test_inode_roundtrip() {
        let inode = Inode {
            id: 12345,
            inode_type: InodeType::File as u8,
            permission: 0o644,
            uid: 1000,
            gid: 100,
            nlink: 1,
            length: 4096,
            atime_ns: 1000000000,
            mtime_ns: 2000000000,
            ctime_ns: 3000000000,
            iflags: 0,
            layout: Some(Layout {
                chain_id: 1,
                stripe_size: 64 * 1024,
                num_stripes: 4,
            }),
            symlink_target: None,
        };
        assert_eq!(roundtrip(&inode), inode);
    }

    #[test]
    fn test_dir_entry_roundtrip() {
        let entry = DirEntry {
            name: "test.txt".to_string(),
            inode_id: 999,
            inode_type: InodeType::File as u8,
        };
        assert_eq!(roundtrip(&entry), entry);
    }

    #[test]
    fn test_lock_action_roundtrip() {
        assert_eq!(roundtrip(&LockAction::TryLock), LockAction::TryLock);
        assert_eq!(roundtrip(&LockAction::Clear), LockAction::Clear);
    }

    #[test]
    fn test_session_info_roundtrip() {
        let s = SessionInfo {
            client: ClientId { high: 1, low: 2 },
            session: ClientId { high: 3, low: 4 },
        };
        assert_eq!(roundtrip(&s), s);
    }
}
