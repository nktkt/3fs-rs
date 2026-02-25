use std::fmt;
use std::str::FromStr;

use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};

/// Network address type.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, IntoPrimitive, TryFromPrimitive, Serialize, Deserialize,
)]
#[repr(u16)]
pub enum AddressType {
    TCP = 0,
    RDMA = 1,
    IPoIB = 2,
    LOCAL = 3,
    UNIX = 4,
}

impl fmt::Display for AddressType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AddressType::TCP => write!(f, "TCP"),
            AddressType::RDMA => write!(f, "RDMA"),
            AddressType::IPoIB => write!(f, "IPoIB"),
            AddressType::LOCAL => write!(f, "LOCAL"),
            AddressType::UNIX => write!(f, "UNIX"),
        }
    }
}

/// Network address combining IP (network byte order), port, and transport type.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Address {
    /// IPv4 address in network byte order (big-endian).
    pub ip: u32,
    /// Port number.
    pub port: u16,
    /// Transport type.
    pub addr_type: AddressType,
}

impl Address {
    /// Create a new address.
    pub fn new(ip: u32, port: u16, addr_type: AddressType) -> Self {
        Self {
            ip,
            port,
            addr_type,
        }
    }

    /// Create an address from four octets, a port, and a type.
    pub fn from_octets(a: u8, b: u8, c: u8, d: u8, port: u16, addr_type: AddressType) -> Self {
        let ip = u32::from_be_bytes([a, b, c, d]);
        Self {
            ip,
            port,
            addr_type,
        }
    }

    /// Return the four IP octets.
    pub fn octets(&self) -> [u8; 4] {
        self.ip.to_be_bytes()
    }

    /// Pack into a u64 for compact storage.
    ///
    /// Layout: `[ip:32][port:16][addr_type:16]`
    pub fn to_u64(&self) -> u64 {
        let ty: u16 = self.addr_type.into();
        ((self.ip as u64) << 32) | ((self.port as u64) << 16) | (ty as u64)
    }

    /// Unpack from a u64.
    pub fn from_u64(val: u64) -> Option<Self> {
        let ip = (val >> 32) as u32;
        let port = ((val >> 16) & 0xFFFF) as u16;
        let ty = (val & 0xFFFF) as u16;
        let addr_type = AddressType::try_from(ty).ok()?;
        Some(Self {
            ip,
            port,
            addr_type,
        })
    }
}

impl fmt::Debug for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let [a, b, c, d] = self.octets();
        write!(f, "{}://{}.{}.{}.{}:{}", self.addr_type, a, b, c, d, self.port)
    }
}

impl FromStr for Address {
    type Err = AddressParseError;

    /// Parse an address string like `"TCP://192.168.1.1:8080"`.
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let (type_str, rest) = s.split_once("://").ok_or(AddressParseError::MissingScheme)?;

        let addr_type = match type_str {
            "TCP" => AddressType::TCP,
            "RDMA" => AddressType::RDMA,
            "IPoIB" => AddressType::IPoIB,
            "LOCAL" => AddressType::LOCAL,
            "UNIX" => AddressType::UNIX,
            _ => return Err(AddressParseError::UnknownType(type_str.to_string())),
        };

        let (ip_str, port_str) = rest.rsplit_once(':').ok_or(AddressParseError::MissingPort)?;
        let port: u16 = port_str
            .parse()
            .map_err(|_| AddressParseError::InvalidPort)?;

        let parts: Vec<&str> = ip_str.split('.').collect();
        if parts.len() != 4 {
            return Err(AddressParseError::InvalidIp);
        }
        let octets: std::result::Result<Vec<u8>, _> = parts.iter().map(|p| p.parse()).collect();
        let octets = octets.map_err(|_| AddressParseError::InvalidIp)?;

        Ok(Address::from_octets(
            octets[0], octets[1], octets[2], octets[3], port, addr_type,
        ))
    }
}

/// Errors when parsing an `Address` from a string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AddressParseError {
    #[error("missing '://' scheme separator")]
    MissingScheme,
    #[error("unknown address type: {0}")]
    UnknownType(String),
    #[error("missing port")]
    MissingPort,
    #[error("invalid port number")]
    InvalidPort,
    #[error("invalid IP address")]
    InvalidIp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_address_display() {
        let addr = Address::from_octets(192, 168, 1, 100, 8080, AddressType::TCP);
        assert_eq!(format!("{}", addr), "TCP://192.168.1.100:8080");
    }

    #[test]
    fn test_address_parse() {
        let addr: Address = "RDMA://10.0.0.1:9999".parse().unwrap();
        assert_eq!(addr.addr_type, AddressType::RDMA);
        assert_eq!(addr.octets(), [10, 0, 0, 1]);
        assert_eq!(addr.port, 9999);
    }

    #[test]
    fn test_address_roundtrip_string() {
        let original = Address::from_octets(172, 16, 0, 1, 443, AddressType::IPoIB);
        let s = format!("{}", original);
        let parsed: Address = s.parse().unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn test_address_u64_roundtrip() {
        let addr = Address::from_octets(10, 20, 30, 40, 5000, AddressType::RDMA);
        let packed = addr.to_u64();
        let unpacked = Address::from_u64(packed).unwrap();
        assert_eq!(addr, unpacked);
    }

    #[test]
    fn test_address_parse_errors() {
        assert!("no-scheme".parse::<Address>().is_err());
        assert!("UNKNOWN://1.2.3.4:80".parse::<Address>().is_err());
        assert!("TCP://1.2.3.4".parse::<Address>().is_err());
        assert!("TCP://1.2.3:80".parse::<Address>().is_err());
        assert!("TCP://1.2.3.4:99999".parse::<Address>().is_err());
    }

    #[test]
    fn test_address_serde() {
        let addr = Address::from_octets(127, 0, 0, 1, 3000, AddressType::LOCAL);
        let json = serde_json::to_string(&addr).unwrap();
        let parsed: Address = serde_json::from_str(&json).unwrap();
        assert_eq!(addr, parsed);
    }
}
