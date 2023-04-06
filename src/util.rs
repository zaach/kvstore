use std::path::{Component, Path, PathBuf};

// from https://github.com/Canop/broot/blob/master/src/path/normalize.rs
/// Improve the path to try remove and solve .. token.
///
/// This assumes that `a/b/../c` is `a/c` which might be different from
/// what the OS would have chosen when b is a link. This is OK
/// for broot verb arguments but can't be generally used elsewhere
/// (a more general solution would probably query the FS and just
/// resolve b in case of links).
///
/// This function ensures a given path ending with '/' still
/// ends with '/' after normalization.
pub fn normalize_path<P: AsRef<Path>>(path: P) -> PathBuf {
    let ends_with_slash = path.as_ref()
        .to_str()
        .map_or(false, |s| s.ends_with('/'));
    let mut normalized = PathBuf::new();
    for component in path.as_ref().components() {
        match &component {
            Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push(component);
                }
            }
            _ => {
                normalized.push(component);
            }
        }
    }
    if ends_with_slash {
        normalized.push("");
    }
    normalized
}
