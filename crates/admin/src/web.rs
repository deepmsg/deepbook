//! The static dashboard's three files.
//!
//! Only a fixed allow-list is served; there is no directory traversal and no
//! content negotiation. The UI is a build-free HTML/JS/CSS trio.

use std::path::Path;

/// One file to answer with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Asset {
    /// `Content-Type`.
    pub content_type: &'static str,
    /// The bytes.
    pub body: Vec<u8>,
}

/// The asset named by `path`, or `None` for anything not on the allow-list.
#[must_use]
pub fn asset(ui_dir: &Path, path: &str) -> Option<Asset> {
    let (name, content_type) = match path {
        "/" | "/index.html" => ("index.html", "text/html; charset=utf-8"),
        "/app.js" => ("app.js", "text/javascript; charset=utf-8"),
        "/style.css" => ("style.css", "text/css; charset=utf-8"),
        _ => return None,
    };
    let body = std::fs::read(ui_dir.join(name)).ok()?;
    Some(Asset { content_type, body })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_allow_list_is_served() {
        let directory =
            std::env::temp_dir().join(format!("deepbook-admin-web-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("directory");
        std::fs::write(directory.join("index.html"), b"<html></html>").expect("write");
        let index = asset(&directory, "/").expect("index");
        assert_eq!("text/html; charset=utf-8", index.content_type);
        assert_eq!(b"<html></html>", index.body.as_slice());
        assert!(asset(&directory, "/../secrets").is_none());
        assert!(asset(&directory, "/nope.js").is_none());
        std::fs::remove_dir_all(directory).expect("cleanup");
    }
}
