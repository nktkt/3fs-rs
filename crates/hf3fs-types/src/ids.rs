strong_type!(InodeId, u64);
strong_type!(ChainId, u64);
strong_type!(NodeId, u32);
strong_type!(Uid, u32);
strong_type!(Gid, u32);
strong_type!(PartitionId, u32);
strong_type!(ChunkId, u64);
strong_type!(TargetId, u32);
strong_type!(GroupId, u32);
strong_type!(SessionId, u64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inode_id() {
        let id = InodeId(12345);
        assert_eq!(*id, 12345u64);
        assert_eq!(format!("{:?}", id), "InodeId(12345)");
    }

    #[test]
    fn test_node_id() {
        let id = NodeId(1);
        assert_eq!(*id, 1u32);
        let raw: u32 = id.into();
        assert_eq!(raw, 1);
    }

    #[test]
    fn test_target_id() {
        let a = TargetId(10);
        let b = TargetId(20);
        assert!(a < b);
    }

    #[test]
    fn test_session_id_serde() {
        let id = SessionId(999);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "999");
        let parsed: SessionId = serde_json::from_str("999").unwrap();
        assert_eq!(parsed, id);
    }
}
