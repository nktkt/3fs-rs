/// Macro to create a strongly-typed newtype wrapper around a primitive.
///
/// The generated type implements:
/// - `Deref` to the inner type
/// - `From<inner>` and `Into<inner>`
/// - `Display`, `Debug`, `Clone`, `Copy`
/// - `PartialEq`, `Eq`, `Hash`, `PartialOrd`, `Ord`
/// - `Default`
/// - `serde::Serialize` and `serde::Deserialize` (transparent)
#[macro_export]
macro_rules! strong_type {
    ($name:ident, $inner:ty) => {
        #[derive(
            Clone,
            Copy,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            Default,
            serde::Serialize,
            serde::Deserialize,
        )]
        #[serde(transparent)]
        #[repr(transparent)]
        pub struct $name(pub $inner);

        impl ::std::ops::Deref for $name {
            type Target = $inner;

            #[inline]
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl ::std::fmt::Debug for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl From<$inner> for $name {
            #[inline]
            fn from(val: $inner) -> Self {
                Self(val)
            }
        }

        impl From<$name> for $inner {
            #[inline]
            fn from(val: $name) -> Self {
                val.0
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    strong_type!(TestId, u64);

    #[test]
    fn test_strong_type_basic() {
        let id = TestId(42);
        assert_eq!(*id, 42u64);
        assert_eq!(id.0, 42);
    }

    #[test]
    fn test_strong_type_from() {
        let id: TestId = 100u64.into();
        assert_eq!(*id, 100);

        let raw: u64 = id.into();
        assert_eq!(raw, 100);
    }

    #[test]
    fn test_strong_type_display_debug() {
        let id = TestId(7);
        assert_eq!(format!("{}", id), "7");
        assert_eq!(format!("{:?}", id), "TestId(7)");
    }

    #[test]
    fn test_strong_type_eq_ord() {
        let a = TestId(1);
        let b = TestId(2);
        let c = TestId(1);

        assert_eq!(a, c);
        assert_ne!(a, b);
        assert!(a < b);
    }

    #[test]
    fn test_strong_type_hash() {
        let mut set = HashSet::new();
        set.insert(TestId(1));
        set.insert(TestId(2));
        set.insert(TestId(1));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_strong_type_default() {
        let id = TestId::default();
        assert_eq!(*id, 0);
    }

    #[test]
    fn test_strong_type_serde() {
        let id = TestId(42);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "42");
        let parsed: TestId = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_strong_type_copy() {
        let a = TestId(5);
        let b = a; // Copy
        assert_eq!(a, b);
    }
}
