use std::path::PathBuf;

use crate::Error;
use crate::sync::{SinkWrapper, SourceWrapper};
use crate::sync::fs::{FsSinkWrapper, FsSourceWrapper};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SshLocation {
    pub user: Option<String>,
    pub host: String,
    pub path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Location {
    Local(PathBuf),
    Ssh(SshLocation),
    Http(String),
}

impl Location {
    pub fn parse(s: &str) -> Option<Location> {
        if s.starts_with("http://") || s.starts_with("https://") {
            Some(Location::Http(s.into()))
        } else if s.starts_with("ssh://") {
            let idx_colon = match s[6..].find(':') {
                Some(i) => i + 6,
                None => return None,
            };
            let (user, host) = match s[6..].find('@') {
                Some(idx_at) if idx_at + 6 < idx_colon => {
                    let idx_at = idx_at + 6;
                    (Some(&s[6 .. idx_at]), &s[idx_at + 1 .. idx_colon])
                }
                _ => (None, &s[6 .. idx_colon]),
            };
            let path = &s[idx_colon + 1 ..];

            Some(Location::Ssh(SshLocation {
                user: user.map(Into::into),
                host: host.into(),
                path: path.into(),
            }))
        } else if s.starts_with("file:///") {
            // FIXME: Unquote path?
            Some(Location::Local(s[7..].into()))
        } else {
            // Return None if starts with [a-z]+:/
            for (i, c) in s.char_indices() {
                if c == ':' {
                    if i > 0 && &s[i + 1..i + 2] == "/" {
                        return None;
                    }
                } else if !c.is_ascii_alphabetic() {
                    break;
                }
            }

            Some(Location::Local(s.into()))
        }
    }

    pub fn open_sink(&self) -> Result<Box<dyn SinkWrapper>, Error> {
        let w = match self {
            Location::Local(path) => Box::new(FsSinkWrapper::new(path)?),
            Location::Ssh(_ssh) => unimplemented!(), // TODO: SSH
            Location::Http(_url) => {
                // Shouldn't happen, caught in main.rs
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Can't write to HTTP location",
                )));
            }
        };
        Ok(w)
    }

    pub fn open_source(&self) -> Result<Box<dyn SourceWrapper>, Error> {
        let w = match self {
            Location::Local(path) => Box::new(FsSourceWrapper::new(path)?),
            Location::Ssh(_ssh) => unimplemented!(), // TODO: SSH
            Location::Http(_url) => unimplemented!(), // TODO: HTTP
        };
        Ok(w)
    }
}

#[cfg(test)]
mod tests {
    use super::{Location, SshLocation};

    #[test]
    fn test_parse() {
        assert_eq!(
            Location::parse("http://example.org/"),
            Some(Location::Http("http://example.org/".into())),
        );
        assert_eq!(
            Location::parse("some/local/path"),
            Some(Location::Local("some/local/path".into())),
        );
        assert_eq!(
            Location::parse("scheme:/local/path"),
            None,
        );
        assert_eq!(
            Location::parse("not-scheme://local/path"),
            Some(Location::Local("not-scheme://local/path".into())),
        );
        assert_eq!(
            Location::parse("notscheme:local/path"),
            Some(Location::Local("notscheme:local/path".into())),
        );
        assert_eq!(
            Location::parse("file:///home/ubuntu/file"),
            Some(Location::Local("/home/ubuntu/file".into())),
        );
        assert_eq!(
            Location::parse("file://file"),
            None,
        );
        assert_eq!(
            Location::parse("ssh://user@host:path"),
            Some(Location::Ssh(SshLocation {
                user: Some("user".into()),
                host: "host".into(),
                path: "path".into(),
            })),
        );
        assert_eq!(
            Location::parse("ssh://host:"),
            Some(Location::Ssh(SshLocation {
                user: None,
                host: "host".into(),
                path: "".into(),
            })),
        );
        assert_eq!(
            Location::parse("ssh://host"),
            None,
        );
    }
}
