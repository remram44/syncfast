//! File locations that we can sync from/to.

use std::path::PathBuf;

use crate::Error;
use crate::sync::{Destination, Source};
//use crate::sync::fs::{FsDestination, FsSource};

/// SSH remote path, with user and host
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SshLocation {
    /// Optional user name. If omitted, local user will be used.
    pub user: Option<String>,
    /// Remote host name
    pub host: String,
    /// Path on the remote machine (may be relative to home)
    pub path: String,
}

/// A location, possible remote, that can be specified by the user
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Location {
    /// A path on the local machine
    Local(PathBuf),
    /// Remote directory accessible via SSH
    Ssh(SshLocation),
    /// Remote HTTP server
    Http(String),
}

impl Location {
    /// Parse a string into a location
    pub fn parse(s: &str) -> Option<Location> {
        if s.starts_with("http://") || s.starts_with("https://") {
            Some(Location::Http(s.into()))
        } else if s.starts_with("ssh://") {
            let idx_colon = match s[6 ..].find(':') {
                Some(i) => i + 6,
                None => return None,
            };
            let (user, host) = match s[6 ..].find('@') {
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
            Some(Location::Local(s[7 ..].into()))
        } else {
            // Return None if starts with [a-z]+:/
            for (i, c) in s.char_indices() {
                if c == ':' {
                    if i > 0 && &s[i + 1 .. i + 2] == "/" {
                        return None;
                    }
                } else if !c.is_ascii_alphabetic() {
                    break;
                }
            }

            Some(Location::Local(s.into()))
        }
    }

    /// Create a `Destination` to sync to this location
    pub fn open_destination(&self) -> Result<Box<dyn Destination>, Error> {
        let w = match self {
            Location::Local(path) => unimplemented!(),//Box::new(FsDestination::new(path.to_owned())?),
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

    /// Create a `Source` to sync from this location
    pub fn open_source(&self) -> Result<Box<dyn Source>, Error> {
        let w = match self {
            Location::Local(path) => unimplemented!(),//Box::new(FsSource::new(path.to_owned())?),
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
        assert_eq!(Location::parse("scheme:/local/path"), None);
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
        assert_eq!(Location::parse("file://file"), None);
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
        assert_eq!(Location::parse("ssh://host"), None);
    }
}
