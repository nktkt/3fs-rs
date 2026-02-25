/// Status code type alias matching C++ `status_code_t = uint16_t`.
#[allow(non_camel_case_types)]
pub type status_code_t = u16;

/// Common status codes (0-999).
pub mod StatusCode {
    use super::status_code_t;

    pub const OK: status_code_t = 0;
    pub const NOT_IMPLEMENTED: status_code_t = 1;
    pub const DATA_CORRUPTION: status_code_t = 2;
    pub const INVALID_ARG: status_code_t = 3;
    pub const INVALID_CONFIG: status_code_t = 4;
    pub const QUEUE_EMPTY: status_code_t = 5;
    pub const QUEUE_FULL: status_code_t = 6;
    pub const QUEUE_CONFLICT: status_code_t = 7;
    pub const QUEUE_INVALID_ITEM: status_code_t = 8;
    pub const MONITOR_INIT_FAILED: status_code_t = 12;
    pub const MONITOR_QUERY_FAILED: status_code_t = 13;
    pub const MONITOR_WRITE_FAILED: status_code_t = 14;
    pub const CONFIG_INVALID_TYPE: status_code_t = 15;
    pub const CONFIG_INVALID_VALUE: status_code_t = 16;
    pub const CONFIG_UPDATE_FAILED: status_code_t = 17;
    pub const CONFIG_VALIDATE_FAILED: status_code_t = 18;
    pub const CONFIG_REDUNDANT_KEY: status_code_t = 19;
    pub const CONFIG_KEY_NOT_FOUND: status_code_t = 20;
    pub const AUTHENTICATION_FAIL: status_code_t = 25;
    pub const NOT_ENOUGH_MEMORY: status_code_t = 26;
    pub const INTERRUPTED: status_code_t = 27;
    pub const INVALID_FORMAT: status_code_t = 33;
    pub const READ_ONLY_MODE: status_code_t = 34;
    pub const SERDE_INSUFFICIENT_LENGTH: status_code_t = 40;
    pub const SERDE_MISSING_FIELDS_AT_END: status_code_t = 41;
    pub const SERDE_VARIANT_INDEX_EXCEEDED: status_code_t = 42;
    pub const SERDE_UNKNOWN_ENUM_VALUE: status_code_t = 43;
    pub const SERDE_NOT_CONTAINER: status_code_t = 44;
    pub const SERDE_NOT_STRING: status_code_t = 45;
    pub const SERDE_NOT_NUMBER: status_code_t = 46;
    pub const SERDE_NOT_INTEGER: status_code_t = 47;
    pub const SERDE_NOT_TABLE: status_code_t = 48;
    pub const SERDE_KEY_NOT_FOUND: status_code_t = 49;
    pub const SERDE_INVALID_JSON: status_code_t = 50;
    pub const SERDE_INVALID_TOML: status_code_t = 51;
    pub const NO_APPLICATION: status_code_t = 52;
    pub const CANNOT_PUSH_CONFIG: status_code_t = 53;
    pub const KV_STORE_NOT_FOUND: status_code_t = 60;
    pub const KV_STORE_GET_ERROR: status_code_t = 61;
    pub const KV_STORE_SET_ERROR: status_code_t = 62;
    pub const KV_STORE_OPEN_FAILED: status_code_t = 63;
    pub const TOKEN_MISMATCH: status_code_t = 64;
    pub const TOKEN_DUPLICATED: status_code_t = 65;
    pub const TOKEN_STALE: status_code_t = 66;
    pub const TOO_MANY_TOKENS: status_code_t = 67;
    pub const KV_STORE_ITERATE_ERROR: status_code_t = 68;
    pub const IO_ERROR: status_code_t = 69;
    pub const FAULT_INJECTION: status_code_t = 70;
    pub const CONFIG_PARSE_ERROR: status_code_t = 71;
    pub const OS_ERROR: status_code_t = 72;
    pub const FOUND_BUG: status_code_t = 998;
    pub const UNKNOWN: status_code_t = 999;
}

/// Transaction status codes (1xxx).
pub mod TransactionCode {
    use super::status_code_t;

    pub const FAILED: status_code_t = 1000;
    pub const CONFLICT: status_code_t = 1001;
    pub const THROTTLED: status_code_t = 1002;
    pub const TOO_OLD: status_code_t = 1003;
    pub const NETWORK_ERROR: status_code_t = 1004;
    pub const CANCELED: status_code_t = 1005;
    pub const MAYBE_COMMITTED: status_code_t = 1006;
    pub const RETRYABLE: status_code_t = 1007;
    pub const RESOURCE_CONSTRAINED: status_code_t = 1008;
    pub const PROCESS_BEHIND: status_code_t = 1009;
    pub const FUTURE_VERSION: status_code_t = 1010;
}

/// RPC status codes (2xxx).
pub mod RPCCode {
    use super::status_code_t;

    pub const INVALID_MESSAGE_TYPE: status_code_t = 2000;
    pub const REQUEST_IS_EMPTY: status_code_t = 2001;
    pub const VERIFY_REQUEST_FAILED: status_code_t = 2002;
    pub const VERIFY_RESPONSE_FAILED: status_code_t = 2003;
    pub const TIMEOUT: status_code_t = 2005;
    pub const INVALID_ADDR: status_code_t = 2006;
    pub const SEND_FAILED: status_code_t = 2007;
    pub const INVALID_SERVICE_ID: status_code_t = 2008;
    pub const INVALID_METHOD_ID: status_code_t = 2009;
    pub const SOCKET_ERROR: status_code_t = 2010;
    pub const LISTEN_FAILED: status_code_t = 2011;
    pub const REQUEST_REFUSED: status_code_t = 2012;
    pub const SOCKET_CLOSED: status_code_t = 2013;
    pub const CONNECT_FAILED: status_code_t = 2014;
    pub const IB_INIT_FAILED: status_code_t = 2015;
    pub const IB_DEVICE_NOT_FOUND: status_code_t = 2016;
    pub const RDMA_POST_FAILED: status_code_t = 2017;
    pub const RDMA_ERROR: status_code_t = 2018;
    pub const RDMA_NO_BUF: status_code_t = 2019;
    pub const INVALID_SERVICE_NAME: status_code_t = 2020;
    pub const IB_DEVICE_NOT_INITIALIZED: status_code_t = 2021;
    pub const EPOLL_INIT_ERROR: status_code_t = 2022;
    pub const EPOLL_ADD_ERROR: status_code_t = 2023;
    pub const EPOLL_DEL_ERROR: status_code_t = 2024;
    pub const EPOLL_WAKE_UP_ERROR: status_code_t = 2025;
    pub const EPOLL_WAIT_ERROR: status_code_t = 2026;
    pub const IB_OPEN_PORT_FAILED: status_code_t = 2027;
}

/// Metadata service status codes (3xxx).
pub mod MetaCode {
    use super::status_code_t;

    pub const NOT_FOUND: status_code_t = 3000;
    pub const NOT_EMPTY: status_code_t = 3001;
    pub const NOT_DIRECTORY: status_code_t = 3003;
    pub const TOO_MANY_SYMLINKS: status_code_t = 3005;
    pub const IS_DIRECTORY: status_code_t = 3006;
    pub const EXISTS: status_code_t = 3007;
    pub const NO_PERMISSION: status_code_t = 3008;
    pub const INCONSISTENT: status_code_t = 3009;
    pub const NOT_FILE: status_code_t = 3010;
    pub const BAD_FILE_SYSTEM: status_code_t = 3011;
    pub const INODE_ID_ALLOC_FAILED: status_code_t = 3012;
    pub const INVALID_FILE_LAYOUT: status_code_t = 3013;
    pub const FILE_HAS_HOLE: status_code_t = 3014;
    pub const O_TRUNC_FAILED: status_code_t = 3015;
    pub const MORE_CHUNKS_TO_REMOVE: status_code_t = 3016;
    pub const NAME_TOO_LONG: status_code_t = 3017;
    pub const REQUEST_CANCELED: status_code_t = 3018;
    pub const BUSY: status_code_t = 3019;
    pub const EXPECTED: status_code_t = 3100;
    pub const NO_LOCK: status_code_t = 3101;
    pub const FILE_TOO_LARGE: status_code_t = 3102;
    pub const RETRYABLE: status_code_t = 3200;
    pub const FORWARD_FAILED: status_code_t = 3201;
    pub const FORWARD_TIMEOUT: status_code_t = 3202;
    pub const OPERATION_TIMEOUT: status_code_t = 3203;
    pub const NOT_RETRYABLE: status_code_t = 3300;
    pub const FOUND_BUG: status_code_t = 3999;
}

/// Storage service status codes (4xxx).
pub mod StorageCode {
    use super::status_code_t;

    pub const CHUNK_METADATA_UNPACK_ERROR: status_code_t = 4000;
    pub const CHUNK_METADATA_NOT_FOUND: status_code_t = 4001;
    pub const CHUNK_METADATA_GET_ERROR: status_code_t = 4002;
    pub const CHUNK_METADATA_SET_ERROR: status_code_t = 4003;
    pub const CHUNK_NOT_COMMIT: status_code_t = 4004;
    pub const CHUNK_NOT_CLEAN: status_code_t = 4005;
    pub const CHUNK_STALE_UPDATE: status_code_t = 4006;
    pub const CHUNK_MISSING_UPDATE: status_code_t = 4007;
    pub const CHUNK_COMMITTED_UPDATE: status_code_t = 4008;
    pub const CHUNK_OPEN_FAILED: status_code_t = 4009;
    pub const CHUNK_READ_FAILED: status_code_t = 4010;
    pub const CHUNK_WRITE_FAILED: status_code_t = 4011;
    pub const CHUNK_ADVANCE_UPDATE: status_code_t = 4012;
    pub const CHUNK_APPLY_FAILED: status_code_t = 4013;
    pub const PUNCH_HOLE_FAILED: status_code_t = 4014;
    pub const CHUNK_SIZE_MISMATCH: status_code_t = 4015;
    pub const STORAGE_STAT_FAILED: status_code_t = 4016;
    pub const STORAGE_UUID_MISMATCH: status_code_t = 4017;
    pub const STORAGE_INIT_FAILED: status_code_t = 4018;
    pub const META_STORE_OPEN_FAILED: status_code_t = 4020;
    pub const META_STORE_INVALID_ITERATOR: status_code_t = 4021;
    pub const CHUNK_NOT_READY_TO_REMOVE: status_code_t = 4022;
    pub const CHUNK_STALE_COMMIT: status_code_t = 4023;
    pub const CHUNK_INVALID_CHUNK_SIZE: status_code_t = 4024;
    pub const TARGET_OFFLINE: status_code_t = 4030;
    pub const TARGET_NOT_FOUND: status_code_t = 4031;
    pub const TARGET_STATE_INVALID: status_code_t = 4032;
    pub const NO_SUCCESSOR_TARGET: status_code_t = 4033;
    pub const NO_SUCCESSOR_ADDR: status_code_t = 4034;
    pub const CHUNK_STORE_INIT_FAILED: status_code_t = 4040;
    pub const AIO_SUBMIT_FAILED: status_code_t = 4050;
    pub const AIO_GET_EVENTS_FAILED: status_code_t = 4051;
    pub const BUFFER_SIZE_EXCEEDED: status_code_t = 4060;
    pub const SYNC_START_FAILED: status_code_t = 4070;
    pub const SYNC_SEND_START_FAILED: status_code_t = 4071;
    pub const SYNC_SEND_DONE_FAILED: status_code_t = 4072;
    pub const CHECKSUM_MISMATCH: status_code_t = 4080;
    pub const CHAIN_VERSION_MISMATCH: status_code_t = 4081;
    pub const CHUNK_VERSION_MISMATCH: status_code_t = 4082;
    pub const CHANNEL_IS_LOCKED: status_code_t = 4090;
    pub const CHUNK_IS_LOCKED: status_code_t = 4091;
}

/// Management daemon status codes (5xxx).
pub mod MgmtdCode {
    use super::status_code_t;

    pub const NOT_PRIMARY: status_code_t = 5000;
    pub const NODE_NOT_FOUND: status_code_t = 5001;
    pub const HEARTBEAT_FAIL: status_code_t = 5002;
    pub const CLUSTER_ID_MISMATCH: status_code_t = 5003;
    pub const REGISTER_FAIL: status_code_t = 5004;
    pub const INVALID_ROUTING_INFO_VERSION: status_code_t = 5005;
    pub const INVALID_CONFIG_VERSION: status_code_t = 5006;
    pub const INVALID_CHAIN_TABLE: status_code_t = 5007;
    pub const INVALID_CHAIN_TABLE_VERSION: status_code_t = 5008;
    pub const HEARTBEAT_VERSION_STALE: status_code_t = 5009;
    pub const CLIENT_SESSION_VERSION_STALE: status_code_t = 5010;
    pub const INVALID_TAG: status_code_t = 5011;
    pub const CHAIN_NOT_FOUND: status_code_t = 5012;
    pub const NODE_TYPE_MISMATCH: status_code_t = 5013;
    pub const EXTEND_CLIENT_SESSION_MISMATCH: status_code_t = 5014;
    pub const CLIENT_SESSION_NOT_FOUND: status_code_t = 5015;
    pub const NOT_ADMIN: status_code_t = 5016;
    pub const TARGET_NOT_FOUND: status_code_t = 5017;
    pub const TARGET_EXISTED: status_code_t = 5018;
}

/// Management daemon client status codes (6xxx).
pub mod MgmtdClientCode {
    use super::status_code_t;

    pub const PRIMARY_MGMTD_NOT_FOUND: status_code_t = 6000;
    pub const WORK_QUEUE_FULL: status_code_t = 6001;
    pub const META_SERVICE_NOT_AVAILABLE: status_code_t = 6002;
    pub const EXIT: status_code_t = 6003;
    pub const ROUTING_INFO_NOT_READY: status_code_t = 6004;
}

/// Storage client status codes (7xxx).
pub mod StorageClientCode {
    use super::status_code_t;

    pub const INIT_FAILED: status_code_t = 7000;
    pub const MEMORY_ERROR: status_code_t = 7001;
    pub const INVALID_ARG: status_code_t = 7002;
    pub const NOT_INITIALIZED: status_code_t = 7003;
    pub const ROUTING_ERROR: status_code_t = 7004;
    pub const NOT_AVAILABLE: status_code_t = 7005;
    pub const COMM_ERROR: status_code_t = 7006;
    pub const CHUNK_NOT_FOUND: status_code_t = 7007;
    pub const TIMEOUT: status_code_t = 7008;
    pub const BAD_CONFIG: status_code_t = 7009;
    pub const REMOTE_IO_ERROR: status_code_t = 7010;
    pub const SERVER_ERROR: status_code_t = 7011;
    pub const RESOURCE_BUSY: status_code_t = 7012;
    pub const DUPLICATE_UPDATE: status_code_t = 7013;
    pub const ROUTING_VERSION_MISMATCH: status_code_t = 7014;
    pub const CHECKSUM_MISMATCH: status_code_t = 7015;
    pub const NO_RDMA_INTERFACE: status_code_t = 7016;
    pub const PROTOCOL_MISMATCH: status_code_t = 7017;
    pub const REQUEST_CANCELED: status_code_t = 7018;
    pub const READ_ONLY_SERVER: status_code_t = 7019;
    pub const CHUNK_NOT_COMMIT: status_code_t = 7020;
    pub const NO_SPACE: status_code_t = 7021;
    pub const FOUND_BUG: status_code_t = 7999;
}

/// Client agent / FUSE status codes (8xxx).
pub mod ClientAgentCode {
    use super::status_code_t;

    pub const TOO_MANY_OPEN_FILES: status_code_t = 8000;
    pub const HOLE_IN_IO_OUTCOME: status_code_t = 8001;
    pub const FAILED_TO_START: status_code_t = 8002;
    pub const OPERATION_DISABLED: status_code_t = 8003;
    pub const IOV_NOT_REGISTERED: status_code_t = 8004;
    pub const IOV_SHM_FAIL: status_code_t = 8006;
}

/// CLI status codes (10xxx).
pub mod CliCode {
    use super::status_code_t;

    pub const WRONG_USAGE: status_code_t = 10000;
}

/// KV service status codes (11xxx).
pub mod KvServiceCode {
    use super::status_code_t;

    pub const UPDATE_CONFLICT: status_code_t = 11000;
    pub const OPERATING_BY_OTHERS: status_code_t = 11001;
    pub const STORE_NOT_FOUND: status_code_t = 11002;
    pub const STORE_NOT_AVAILABLE: status_code_t = 11003;
    pub const STORE_LOADED: status_code_t = 11004;
    pub const CLUSTER_ID_MISMATCH: status_code_t = 11005;
    pub const NOT_PRIMARY: status_code_t = 11006;
    pub const TABLE_INFO_STALE: status_code_t = 11007;
    pub const HEARTBEAT_VERSION_STALE: status_code_t = 11008;
    pub const UNKNOWN_WORKER: status_code_t = 11009;
    pub const STORE_LEASE_EMPTY: status_code_t = 11010;
    pub const STORE_LEASE_HELD: status_code_t = 11011;
    pub const TABLE_NOT_FOUND: status_code_t = 11012;
    pub const TABLE_ID_MISMATCH: status_code_t = 11013;
    pub const NO_AVAILABLE_WORKER: status_code_t = 11014;
    pub const NO_PERMISSION: status_code_t = 11015;
    pub const NO_AVAILABLE_MASTER: status_code_t = 11016;
    pub const STORE_TYPE_MISMATCH: status_code_t = 11017;
    pub const WRITE_STALE: status_code_t = 11018;
    pub const EXISTS: status_code_t = 11019;
    pub const NOT_FOUND: status_code_t = 11020;
    pub const OUT_OF_RANGE: status_code_t = 11021;
    pub const DELETED: status_code_t = 11022;
    pub const SERVICE_STOPPING: status_code_t = 11023;
}

/// Classification of status code ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum StatusCodeType {
    Invalid = -1,
    Common = 0,
    Transaction = 1,
    RPC = 2,
    Meta = 3,
    Storage = 4,
    Mgmtd = 5,
    MgmtdClient = 6,
    StorageClient = 7,
    ClientAgent = 8,
    Cli = 9,
    KvService = 10,
}

/// Determine the type/category of a status code.
pub fn type_of(code: status_code_t) -> StatusCodeType {
    match code {
        0..=999 => StatusCodeType::Common,
        1000..=1999 => StatusCodeType::Transaction,
        2000..=2999 => StatusCodeType::RPC,
        3000..=3999 => StatusCodeType::Meta,
        4000..=4999 => StatusCodeType::Storage,
        5000..=5999 => StatusCodeType::Mgmtd,
        6000..=6999 => StatusCodeType::MgmtdClient,
        7000..=7999 => StatusCodeType::StorageClient,
        8000..=8999 => StatusCodeType::ClientAgent,
        10000..=10999 => StatusCodeType::Cli,
        11000..=11999 => StatusCodeType::KvService,
        _ => StatusCodeType::Invalid,
    }
}

/// Convert a status code to its human-readable name.
pub fn to_string(code: status_code_t) -> &'static str {
    match code {
        // Common
        StatusCode::OK => "OK",
        StatusCode::NOT_IMPLEMENTED => "NotImplemented",
        StatusCode::DATA_CORRUPTION => "DataCorruption",
        StatusCode::INVALID_ARG => "InvalidArg",
        StatusCode::INVALID_CONFIG => "InvalidConfig",
        StatusCode::QUEUE_EMPTY => "QueueEmpty",
        StatusCode::QUEUE_FULL => "QueueFull",
        StatusCode::QUEUE_CONFLICT => "QueueConflict",
        StatusCode::QUEUE_INVALID_ITEM => "QueueInvalidItem",
        StatusCode::MONITOR_INIT_FAILED => "MonitorInitFailed",
        StatusCode::MONITOR_QUERY_FAILED => "MonitorQueryFailed",
        StatusCode::MONITOR_WRITE_FAILED => "MonitorWriteFailed",
        StatusCode::CONFIG_INVALID_TYPE => "ConfigInvalidType",
        StatusCode::CONFIG_INVALID_VALUE => "ConfigInvalidValue",
        StatusCode::CONFIG_UPDATE_FAILED => "ConfigUpdateFailed",
        StatusCode::CONFIG_VALIDATE_FAILED => "ConfigValidateFailed",
        StatusCode::CONFIG_REDUNDANT_KEY => "ConfigRedundantKey",
        StatusCode::CONFIG_KEY_NOT_FOUND => "ConfigKeyNotFound",
        StatusCode::AUTHENTICATION_FAIL => "AuthenticationFail",
        StatusCode::NOT_ENOUGH_MEMORY => "NotEnoughMemory",
        StatusCode::INTERRUPTED => "Interrupted",
        StatusCode::INVALID_FORMAT => "InvalidFormat",
        StatusCode::READ_ONLY_MODE => "ReadOnlyMode",
        StatusCode::SERDE_INSUFFICIENT_LENGTH => "SerdeInsufficientLength",
        StatusCode::SERDE_MISSING_FIELDS_AT_END => "SerdeMissingFieldsAtEnd",
        StatusCode::SERDE_VARIANT_INDEX_EXCEEDED => "SerdeVariantIndexExceeded",
        StatusCode::SERDE_UNKNOWN_ENUM_VALUE => "SerdeUnknownEnumValue",
        StatusCode::SERDE_NOT_CONTAINER => "SerdeNotContainer",
        StatusCode::SERDE_NOT_STRING => "SerdeNotString",
        StatusCode::SERDE_NOT_NUMBER => "SerdeNotNumber",
        StatusCode::SERDE_NOT_INTEGER => "SerdeNotInteger",
        StatusCode::SERDE_NOT_TABLE => "SerdeNotTable",
        StatusCode::SERDE_KEY_NOT_FOUND => "SerdeKeyNotFound",
        StatusCode::SERDE_INVALID_JSON => "SerdeInvalidJson",
        StatusCode::SERDE_INVALID_TOML => "SerdeInvalidToml",
        StatusCode::NO_APPLICATION => "NoApplication",
        StatusCode::CANNOT_PUSH_CONFIG => "CannotPushConfig",
        StatusCode::KV_STORE_NOT_FOUND => "KVStoreNotFound",
        StatusCode::KV_STORE_GET_ERROR => "KVStoreGetError",
        StatusCode::KV_STORE_SET_ERROR => "KVStoreSetError",
        StatusCode::KV_STORE_OPEN_FAILED => "KVStoreOpenFailed",
        StatusCode::TOKEN_MISMATCH => "TokenMismatch",
        StatusCode::TOKEN_DUPLICATED => "TokenDuplicated",
        StatusCode::TOKEN_STALE => "TokenStale",
        StatusCode::TOO_MANY_TOKENS => "TooManyTokens",
        StatusCode::KV_STORE_ITERATE_ERROR => "KVStoreIterateError",
        StatusCode::IO_ERROR => "IOError",
        StatusCode::FAULT_INJECTION => "FaultInjection",
        StatusCode::CONFIG_PARSE_ERROR => "ConfigParseError",
        StatusCode::OS_ERROR => "OSError",
        StatusCode::FOUND_BUG => "FoundBug",
        StatusCode::UNKNOWN => "Unknown",

        // Transaction
        TransactionCode::FAILED => "Transaction::Failed",
        TransactionCode::CONFLICT => "Transaction::Conflict",
        TransactionCode::THROTTLED => "Transaction::Throttled",
        TransactionCode::TOO_OLD => "Transaction::TooOld",
        TransactionCode::NETWORK_ERROR => "Transaction::NetworkError",
        TransactionCode::CANCELED => "Transaction::Canceled",
        TransactionCode::MAYBE_COMMITTED => "Transaction::MaybeCommitted",
        TransactionCode::RETRYABLE => "Transaction::Retryable",
        TransactionCode::RESOURCE_CONSTRAINED => "Transaction::ResourceConstrained",
        TransactionCode::PROCESS_BEHIND => "Transaction::ProcessBehind",
        TransactionCode::FUTURE_VERSION => "Transaction::FutureVersion",

        // RPC
        RPCCode::INVALID_MESSAGE_TYPE => "RPC::InvalidMessageType",
        RPCCode::REQUEST_IS_EMPTY => "RPC::RequestIsEmpty",
        RPCCode::VERIFY_REQUEST_FAILED => "RPC::VerifyRequestFailed",
        RPCCode::VERIFY_RESPONSE_FAILED => "RPC::VerifyResponseFailed",
        RPCCode::TIMEOUT => "RPC::Timeout",
        RPCCode::INVALID_ADDR => "RPC::InvalidAddr",
        RPCCode::SEND_FAILED => "RPC::SendFailed",
        RPCCode::INVALID_SERVICE_ID => "RPC::InvalidServiceID",
        RPCCode::INVALID_METHOD_ID => "RPC::InvalidMethodID",
        RPCCode::SOCKET_ERROR => "RPC::SocketError",
        RPCCode::LISTEN_FAILED => "RPC::ListenFailed",
        RPCCode::REQUEST_REFUSED => "RPC::RequestRefused",
        RPCCode::SOCKET_CLOSED => "RPC::SocketClosed",
        RPCCode::CONNECT_FAILED => "RPC::ConnectFailed",
        RPCCode::IB_INIT_FAILED => "RPC::IBInitFailed",
        RPCCode::IB_DEVICE_NOT_FOUND => "RPC::IBDeviceNotFound",
        RPCCode::RDMA_POST_FAILED => "RPC::RDMAPostFailed",
        RPCCode::RDMA_ERROR => "RPC::RDMAError",
        RPCCode::RDMA_NO_BUF => "RPC::RDMANoBuf",
        RPCCode::INVALID_SERVICE_NAME => "RPC::InvalidServiceName",
        RPCCode::IB_DEVICE_NOT_INITIALIZED => "RPC::IBDeviceNotInitialized",
        RPCCode::EPOLL_INIT_ERROR => "RPC::EpollInitError",
        RPCCode::EPOLL_ADD_ERROR => "RPC::EpollAddError",
        RPCCode::EPOLL_DEL_ERROR => "RPC::EpollDelError",
        RPCCode::EPOLL_WAKE_UP_ERROR => "RPC::EpollWakeUpError",
        RPCCode::EPOLL_WAIT_ERROR => "RPC::EpollWaitError",
        RPCCode::IB_OPEN_PORT_FAILED => "RPC::IBOpenPortFailed",

        // Meta
        MetaCode::NOT_FOUND => "Meta::NotFound",
        MetaCode::NOT_EMPTY => "Meta::NotEmpty",
        MetaCode::NOT_DIRECTORY => "Meta::NotDirectory",
        MetaCode::TOO_MANY_SYMLINKS => "Meta::TooManySymlinks",
        MetaCode::IS_DIRECTORY => "Meta::IsDirectory",
        MetaCode::EXISTS => "Meta::Exists",
        MetaCode::NO_PERMISSION => "Meta::NoPermission",
        MetaCode::INCONSISTENT => "Meta::Inconsistent",
        MetaCode::NOT_FILE => "Meta::NotFile",
        MetaCode::BAD_FILE_SYSTEM => "Meta::BadFileSystem",
        MetaCode::INODE_ID_ALLOC_FAILED => "Meta::InodeIdAllocFailed",
        MetaCode::INVALID_FILE_LAYOUT => "Meta::InvalidFileLayout",
        MetaCode::FILE_HAS_HOLE => "Meta::FileHasHole",
        MetaCode::O_TRUNC_FAILED => "Meta::OTruncFailed",
        MetaCode::MORE_CHUNKS_TO_REMOVE => "Meta::MoreChunksToRemove",
        MetaCode::NAME_TOO_LONG => "Meta::NameTooLong",
        MetaCode::REQUEST_CANCELED => "Meta::RequestCanceled",
        MetaCode::BUSY => "Meta::Busy",
        MetaCode::EXPECTED => "Meta::Expected",
        MetaCode::NO_LOCK => "Meta::NoLock",
        MetaCode::FILE_TOO_LARGE => "Meta::FileTooLarge",
        MetaCode::RETRYABLE => "Meta::Retryable",
        MetaCode::FORWARD_FAILED => "Meta::ForwardFailed",
        MetaCode::FORWARD_TIMEOUT => "Meta::ForwardTimeout",
        MetaCode::OPERATION_TIMEOUT => "Meta::OperationTimeout",
        MetaCode::NOT_RETRYABLE => "Meta::NotRetryable",
        MetaCode::FOUND_BUG => "Meta::FoundBug",

        // Storage
        StorageCode::CHUNK_METADATA_UNPACK_ERROR => "Storage::ChunkMetadataUnpackError",
        StorageCode::CHUNK_METADATA_NOT_FOUND => "Storage::ChunkMetadataNotFound",
        StorageCode::CHUNK_METADATA_GET_ERROR => "Storage::ChunkMetadataGetError",
        StorageCode::CHUNK_METADATA_SET_ERROR => "Storage::ChunkMetadataSetError",
        StorageCode::CHUNK_NOT_COMMIT => "Storage::ChunkNotCommit",
        StorageCode::CHUNK_NOT_CLEAN => "Storage::ChunkNotClean",
        StorageCode::CHUNK_STALE_UPDATE => "Storage::ChunkStaleUpdate",
        StorageCode::CHUNK_MISSING_UPDATE => "Storage::ChunkMissingUpdate",
        StorageCode::CHUNK_COMMITTED_UPDATE => "Storage::ChunkCommittedUpdate",
        StorageCode::CHUNK_OPEN_FAILED => "Storage::ChunkOpenFailed",
        StorageCode::CHUNK_READ_FAILED => "Storage::ChunkReadFailed",
        StorageCode::CHUNK_WRITE_FAILED => "Storage::ChunkWriteFailed",
        StorageCode::CHUNK_ADVANCE_UPDATE => "Storage::ChunkAdvanceUpdate",
        StorageCode::CHUNK_APPLY_FAILED => "Storage::ChunkApplyFailed",
        StorageCode::PUNCH_HOLE_FAILED => "Storage::PunchHoleFailed",
        StorageCode::CHUNK_SIZE_MISMATCH => "Storage::ChunkSizeMismatch",
        StorageCode::STORAGE_STAT_FAILED => "Storage::StorageStatFailed",
        StorageCode::STORAGE_UUID_MISMATCH => "Storage::StorageUUIDMismatch",
        StorageCode::STORAGE_INIT_FAILED => "Storage::StorageInitFailed",
        StorageCode::META_STORE_OPEN_FAILED => "Storage::MetaStoreOpenFailed",
        StorageCode::META_STORE_INVALID_ITERATOR => "Storage::MetaStoreInvalidIterator",
        StorageCode::CHUNK_NOT_READY_TO_REMOVE => "Storage::ChunkNotReadyToRemove",
        StorageCode::CHUNK_STALE_COMMIT => "Storage::ChunkStaleCommit",
        StorageCode::CHUNK_INVALID_CHUNK_SIZE => "Storage::ChunkInvalidChunkSize",
        StorageCode::TARGET_OFFLINE => "Storage::TargetOffline",
        StorageCode::TARGET_NOT_FOUND => "Storage::TargetNotFound",
        StorageCode::TARGET_STATE_INVALID => "Storage::TargetStateInvalid",
        StorageCode::NO_SUCCESSOR_TARGET => "Storage::NoSuccessorTarget",
        StorageCode::NO_SUCCESSOR_ADDR => "Storage::NoSuccessorAddr",
        StorageCode::CHUNK_STORE_INIT_FAILED => "Storage::ChunkStoreInitFailed",
        StorageCode::AIO_SUBMIT_FAILED => "Storage::AioSubmitFailed",
        StorageCode::AIO_GET_EVENTS_FAILED => "Storage::AioGetEventsFailed",
        StorageCode::BUFFER_SIZE_EXCEEDED => "Storage::BufferSizeExceeded",
        StorageCode::SYNC_START_FAILED => "Storage::SyncStartFailed",
        StorageCode::SYNC_SEND_START_FAILED => "Storage::SyncSendStartFailed",
        StorageCode::SYNC_SEND_DONE_FAILED => "Storage::SyncSendDoneFailed",
        StorageCode::CHECKSUM_MISMATCH => "Storage::ChecksumMismatch",
        StorageCode::CHAIN_VERSION_MISMATCH => "Storage::ChainVersionMismatch",
        StorageCode::CHUNK_VERSION_MISMATCH => "Storage::ChunkVersionMismatch",
        StorageCode::CHANNEL_IS_LOCKED => "Storage::ChannelIsLocked",
        StorageCode::CHUNK_IS_LOCKED => "Storage::ChunkIsLocked",

        // Mgmtd
        MgmtdCode::NOT_PRIMARY => "Mgmtd::NotPrimary",
        MgmtdCode::NODE_NOT_FOUND => "Mgmtd::NodeNotFound",
        MgmtdCode::HEARTBEAT_FAIL => "Mgmtd::HeartbeatFail",
        MgmtdCode::CLUSTER_ID_MISMATCH => "Mgmtd::ClusterIdMismatch",
        MgmtdCode::REGISTER_FAIL => "Mgmtd::RegisterFail",
        MgmtdCode::INVALID_ROUTING_INFO_VERSION => "Mgmtd::InvalidRoutingInfoVersion",
        MgmtdCode::INVALID_CONFIG_VERSION => "Mgmtd::InvalidConfigVersion",
        MgmtdCode::INVALID_CHAIN_TABLE => "Mgmtd::InvalidChainTable",
        MgmtdCode::INVALID_CHAIN_TABLE_VERSION => "Mgmtd::InvalidChainTableVersion",
        MgmtdCode::HEARTBEAT_VERSION_STALE => "Mgmtd::HeartbeatVersionStale",
        MgmtdCode::CLIENT_SESSION_VERSION_STALE => "Mgmtd::ClientSessionVersionStale",
        MgmtdCode::INVALID_TAG => "Mgmtd::InvalidTag",
        MgmtdCode::CHAIN_NOT_FOUND => "Mgmtd::ChainNotFound",
        MgmtdCode::NODE_TYPE_MISMATCH => "Mgmtd::NodeTypeMismatch",
        MgmtdCode::EXTEND_CLIENT_SESSION_MISMATCH => "Mgmtd::ExtendClientSessionMismatch",
        MgmtdCode::CLIENT_SESSION_NOT_FOUND => "Mgmtd::ClientSessionNotFound",
        MgmtdCode::NOT_ADMIN => "Mgmtd::NotAdmin",
        MgmtdCode::TARGET_NOT_FOUND => "Mgmtd::TargetNotFound",
        MgmtdCode::TARGET_EXISTED => "Mgmtd::TargetExisted",

        // MgmtdClient
        MgmtdClientCode::PRIMARY_MGMTD_NOT_FOUND => "MgmtdClient::PrimaryMgmtdNotFound",
        MgmtdClientCode::WORK_QUEUE_FULL => "MgmtdClient::WorkQueueFull",
        MgmtdClientCode::META_SERVICE_NOT_AVAILABLE => "MgmtdClient::MetaServiceNotAvailable",
        MgmtdClientCode::EXIT => "MgmtdClient::Exit",
        MgmtdClientCode::ROUTING_INFO_NOT_READY => "MgmtdClient::RoutingInfoNotReady",

        // StorageClient
        StorageClientCode::INIT_FAILED => "StorageClient::InitFailed",
        StorageClientCode::MEMORY_ERROR => "StorageClient::MemoryError",
        StorageClientCode::INVALID_ARG => "StorageClient::InvalidArg",
        StorageClientCode::NOT_INITIALIZED => "StorageClient::NotInitialized",
        StorageClientCode::ROUTING_ERROR => "StorageClient::RoutingError",
        StorageClientCode::NOT_AVAILABLE => "StorageClient::NotAvailable",
        StorageClientCode::COMM_ERROR => "StorageClient::CommError",
        StorageClientCode::CHUNK_NOT_FOUND => "StorageClient::ChunkNotFound",
        StorageClientCode::TIMEOUT => "StorageClient::Timeout",
        StorageClientCode::BAD_CONFIG => "StorageClient::BadConfig",
        StorageClientCode::REMOTE_IO_ERROR => "StorageClient::RemoteIOError",
        StorageClientCode::SERVER_ERROR => "StorageClient::ServerError",
        StorageClientCode::RESOURCE_BUSY => "StorageClient::ResourceBusy",
        StorageClientCode::DUPLICATE_UPDATE => "StorageClient::DuplicateUpdate",
        StorageClientCode::ROUTING_VERSION_MISMATCH => "StorageClient::RoutingVersionMismatch",
        StorageClientCode::CHECKSUM_MISMATCH => "StorageClient::ChecksumMismatch",
        StorageClientCode::NO_RDMA_INTERFACE => "StorageClient::NoRDMAInterface",
        StorageClientCode::PROTOCOL_MISMATCH => "StorageClient::ProtocolMismatch",
        StorageClientCode::REQUEST_CANCELED => "StorageClient::RequestCanceled",
        StorageClientCode::READ_ONLY_SERVER => "StorageClient::ReadOnlyServer",
        StorageClientCode::CHUNK_NOT_COMMIT => "StorageClient::ChunkNotCommit",
        StorageClientCode::NO_SPACE => "StorageClient::NoSpace",
        StorageClientCode::FOUND_BUG => "StorageClient::FoundBug",

        // ClientAgent
        ClientAgentCode::TOO_MANY_OPEN_FILES => "ClientAgent::TooManyOpenFiles",
        ClientAgentCode::HOLE_IN_IO_OUTCOME => "ClientAgent::HoleInIoOutcome",
        ClientAgentCode::FAILED_TO_START => "ClientAgent::FailedToStart",
        ClientAgentCode::OPERATION_DISABLED => "ClientAgent::OperationDisabled",
        ClientAgentCode::IOV_NOT_REGISTERED => "ClientAgent::IovNotRegistered",
        ClientAgentCode::IOV_SHM_FAIL => "ClientAgent::IovShmFail",

        // Cli
        CliCode::WRONG_USAGE => "Cli::WrongUsage",

        // KvService
        KvServiceCode::UPDATE_CONFLICT => "KvService::UpdateConflict",
        KvServiceCode::OPERATING_BY_OTHERS => "KvService::OperatingByOthers",
        KvServiceCode::STORE_NOT_FOUND => "KvService::StoreNotFound",
        KvServiceCode::STORE_NOT_AVAILABLE => "KvService::StoreNotAvailable",
        KvServiceCode::STORE_LOADED => "KvService::StoreLoaded",
        KvServiceCode::CLUSTER_ID_MISMATCH => "KvService::ClusterIdMismatch",
        KvServiceCode::NOT_PRIMARY => "KvService::NotPrimary",
        KvServiceCode::TABLE_INFO_STALE => "KvService::TableInfoStale",
        KvServiceCode::HEARTBEAT_VERSION_STALE => "KvService::HeartbeatVersionStale",
        KvServiceCode::UNKNOWN_WORKER => "KvService::UnknownWorker",
        KvServiceCode::STORE_LEASE_EMPTY => "KvService::StoreLeaseEmpty",
        KvServiceCode::STORE_LEASE_HELD => "KvService::StoreLeaseHeld",
        KvServiceCode::TABLE_NOT_FOUND => "KvService::TableNotFound",
        KvServiceCode::TABLE_ID_MISMATCH => "KvService::TableIdMismatch",
        KvServiceCode::NO_AVAILABLE_WORKER => "KvService::NoAvailableWorker",
        KvServiceCode::NO_PERMISSION => "KvService::NoPermission",
        KvServiceCode::NO_AVAILABLE_MASTER => "KvService::NoAvailableMaster",
        KvServiceCode::STORE_TYPE_MISMATCH => "KvService::StoreTypeMismatch",
        KvServiceCode::WRITE_STALE => "KvService::WriteStale",
        KvServiceCode::EXISTS => "KvService::Exists",
        KvServiceCode::NOT_FOUND => "KvService::NotFound",
        KvServiceCode::OUT_OF_RANGE => "KvService::OutOfRange",
        KvServiceCode::DELETED => "KvService::Deleted",
        KvServiceCode::SERVICE_STOPPING => "KvService::ServiceStopping",

        _ => "UnknownStatusCode",
    }
}

/// Convert a status code to the corresponding POSIX errno value.
///
/// Mirrors the C++ `StatusCode::toErrno()` implementation.
/// Uses libc constants on the target platform.
pub fn to_errno(code: status_code_t) -> i32 {
    // All RPC codes map to EIO (remote I/O error)
    if type_of(code) == StatusCodeType::RPC {
        return libc::EIO; // EREMOTEIO is Linux-specific; EIO is portable
    }

    match code {
        c if c == ClientAgentCode::TOO_MANY_OPEN_FILES => libc::EMFILE,

        c if c == StatusCode::INVALID_ARG => libc::EINVAL,
        c if c == StatusCode::NOT_IMPLEMENTED => libc::ENOSYS,
        c if c == StatusCode::NOT_ENOUGH_MEMORY => libc::ENOMEM,
        c if c == StatusCode::AUTHENTICATION_FAIL => libc::EPERM,
        c if c == StatusCode::READ_ONLY_MODE => libc::EROFS,

        c if c == MetaCode::REQUEST_CANCELED || c == StorageClientCode::REQUEST_CANCELED => {
            libc::EINTR
        }
        c if c == StorageClientCode::NO_SPACE => libc::ENOSPC,

        c if c == MetaCode::NOT_FOUND => libc::ENOENT,
        c if c == MetaCode::NOT_EMPTY => libc::ENOTEMPTY,
        c if c == MetaCode::NOT_DIRECTORY => libc::ENOTDIR,
        c if c == MetaCode::TOO_MANY_SYMLINKS => libc::ELOOP,
        c if c == MetaCode::IS_DIRECTORY => libc::EISDIR,
        c if c == MetaCode::EXISTS => libc::EEXIST,
        c if c == MetaCode::NO_PERMISSION => libc::EPERM,
        c if c == MetaCode::INCONSISTENT => libc::EIO,
        c if c == MetaCode::NOT_FILE => libc::EBADF,
        c if c == MetaCode::MORE_CHUNKS_TO_REMOVE => libc::EAGAIN,
        c if c == MetaCode::NAME_TOO_LONG => libc::ENAMETOOLONG,
        c if c == MetaCode::NO_LOCK => libc::ENOLCK,
        c if c == MetaCode::FILE_TOO_LARGE => libc::EFBIG,

        c if c == ClientAgentCode::HOLE_IN_IO_OUTCOME => libc::ENODATA,
        c if c == ClientAgentCode::OPERATION_DISABLED => libc::EOPNOTSUPP,
        c if c == ClientAgentCode::IOV_NOT_REGISTERED => libc::EACCES,
        c if c == ClientAgentCode::IOV_SHM_FAIL => libc::EACCES,

        _ => libc::EIO,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_code_values() {
        assert_eq!(StatusCode::OK, 0);
        assert_eq!(StatusCode::UNKNOWN, 999);
        assert_eq!(TransactionCode::FAILED, 1000);
        assert_eq!(RPCCode::INVALID_MESSAGE_TYPE, 2000);
        assert_eq!(MetaCode::NOT_FOUND, 3000);
        assert_eq!(StorageCode::CHUNK_METADATA_UNPACK_ERROR, 4000);
        assert_eq!(MgmtdCode::NOT_PRIMARY, 5000);
        assert_eq!(MgmtdClientCode::PRIMARY_MGMTD_NOT_FOUND, 6000);
        assert_eq!(StorageClientCode::INIT_FAILED, 7000);
        assert_eq!(ClientAgentCode::TOO_MANY_OPEN_FILES, 8000);
        assert_eq!(CliCode::WRONG_USAGE, 10000);
        assert_eq!(KvServiceCode::UPDATE_CONFLICT, 11000);
    }

    #[test]
    fn test_type_of() {
        assert_eq!(type_of(StatusCode::OK), StatusCodeType::Common);
        assert_eq!(type_of(StatusCode::UNKNOWN), StatusCodeType::Common);
        assert_eq!(type_of(TransactionCode::FAILED), StatusCodeType::Transaction);
        assert_eq!(type_of(RPCCode::TIMEOUT), StatusCodeType::RPC);
        assert_eq!(type_of(MetaCode::NOT_FOUND), StatusCodeType::Meta);
        assert_eq!(type_of(StorageCode::CHUNK_METADATA_UNPACK_ERROR), StatusCodeType::Storage);
        assert_eq!(type_of(MgmtdCode::NOT_PRIMARY), StatusCodeType::Mgmtd);
        assert_eq!(type_of(MgmtdClientCode::PRIMARY_MGMTD_NOT_FOUND), StatusCodeType::MgmtdClient);
        assert_eq!(type_of(StorageClientCode::INIT_FAILED), StatusCodeType::StorageClient);
        assert_eq!(type_of(ClientAgentCode::TOO_MANY_OPEN_FILES), StatusCodeType::ClientAgent);
        assert_eq!(type_of(CliCode::WRONG_USAGE), StatusCodeType::Cli);
        assert_eq!(type_of(KvServiceCode::UPDATE_CONFLICT), StatusCodeType::KvService);
        assert_eq!(type_of(9000), StatusCodeType::Invalid);
        assert_eq!(type_of(65535), StatusCodeType::Invalid);
    }

    #[test]
    fn test_to_string() {
        assert_eq!(to_string(StatusCode::OK), "OK");
        assert_eq!(to_string(StatusCode::INVALID_ARG), "InvalidArg");
        assert_eq!(to_string(RPCCode::TIMEOUT), "RPC::Timeout");
        assert_eq!(to_string(MetaCode::NOT_FOUND), "Meta::NotFound");
        assert_eq!(to_string(StorageCode::CHUNK_IS_LOCKED), "Storage::ChunkIsLocked");
        assert_eq!(to_string(MgmtdCode::TARGET_EXISTED), "Mgmtd::TargetExisted");
        assert_eq!(to_string(KvServiceCode::SERVICE_STOPPING), "KvService::ServiceStopping");
        assert_eq!(to_string(12345), "UnknownStatusCode");
    }

    #[test]
    fn test_to_errno() {
        assert_eq!(to_errno(StatusCode::OK), libc::EIO); // OK is default
        assert_eq!(to_errno(StatusCode::INVALID_ARG), libc::EINVAL);
        assert_eq!(to_errno(StatusCode::NOT_IMPLEMENTED), libc::ENOSYS);
        assert_eq!(to_errno(MetaCode::NOT_FOUND), libc::ENOENT);
        assert_eq!(to_errno(MetaCode::EXISTS), libc::EEXIST);
        assert_eq!(to_errno(MetaCode::NO_PERMISSION), libc::EPERM);
        assert_eq!(to_errno(MetaCode::NAME_TOO_LONG), libc::ENAMETOOLONG);
        assert_eq!(to_errno(RPCCode::TIMEOUT), libc::EIO);
        assert_eq!(to_errno(ClientAgentCode::TOO_MANY_OPEN_FILES), libc::EMFILE);
        assert_eq!(to_errno(StorageClientCode::NO_SPACE), libc::ENOSPC);
    }
}
