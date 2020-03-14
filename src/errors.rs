use std::error::Error as StdError;
use std::fmt;

pub (crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FileExists,
    BucketExists,
    BucketMissing,
    IncompatibleValue,
    ReadOnlyTx,
    IOError(std::io::Error),
    SyncError(&'static str),
}

impl StdError for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::FileExists => write!(f, "File already exists"),
            Error::BucketExists => write!(f, "Bucket already exists"),
            Error::BucketMissing => write!(f, "Bucket does not exist"),
            Error::IncompatibleValue => write!(f, "Value not compatible"),
            Error::ReadOnlyTx => write!(f, "Cannot write in a read-only transaction"),
			Error::IOError(e) => write!(f, "IO Error: {}", e),
			Error::SyncError(s) => write!(f, "Sync Error: {}", s),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IOError(err)
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(_: std::sync::PoisonError<T>) -> Error {
        Error::SyncError("lock poisoned")
    }
}
