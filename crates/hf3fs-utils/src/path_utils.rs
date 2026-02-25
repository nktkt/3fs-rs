use std::path::{Path, PathBuf};

/// Normalize a path by resolving `.` and `..` components without filesystem access.
pub fn normalize_path(path: &Path) -> PathBuf {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                if !components.is_empty() {
                    components.pop();
                }
            }
            std::path::Component::CurDir => {}
            c => components.push(c),
        }
    }
    components.iter().collect()
}

/// Join two paths, handling absolute second path.
pub fn join_path(base: &Path, relative: &Path) -> PathBuf {
    if relative.is_absolute() {
        relative.to_path_buf()
    } else {
        base.join(relative)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize() {
        assert_eq!(
            normalize_path(Path::new("/a/b/../c/./d")),
            PathBuf::from("/a/c/d")
        );
    }

    #[test]
    fn test_join_absolute() {
        assert_eq!(
            join_path(Path::new("/base"), Path::new("/absolute")),
            PathBuf::from("/absolute")
        );
    }

    #[test]
    fn test_join_relative() {
        assert_eq!(
            join_path(Path::new("/base"), Path::new("relative")),
            PathBuf::from("/base/relative")
        );
    }
}
