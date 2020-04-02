use std::error::Error as StdError;
use std::fmt;
use std::sync::PoisonError;

pub(crate) type Result<T> = std::result::Result<T, Error>;

/// Possible database errors
#[derive(Debug)]
pub enum Error {
    /// Tried to create a bucket that already exists
    BucketExists,
    /// Tried to get a bucket that does not exist
    BucketMissing,
    /// Tried to delete a key-value pair that does not exist
    KeyValueMissing,
    /// Tried to get a bucket but found a key / value pair instead, or tried to put a key / value pair but found an existing bucket
    IncompatibleValue,
    /// Tried to write to a read only transaction
    ReadOnlyTx,
    /// Wrapper around a [`std::io::Error`] that occurred while opening the file or writing to it
    IOError(std::io::Error),
    /// Wrapper around a [`PoisonError`]
    SyncError(&'static str),
    /// Error returned when the DB is found to be in an invalid state
    InvalidDB(String),
}

impl StdError for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::BucketExists => write!(f, "Bucket already exists"),
            Error::BucketMissing => write!(f, "Bucket does not exist"),
            Error::KeyValueMissing => write!(f, "Key-Value pair does not exist"),
            Error::IncompatibleValue => write!(f, "Value not compatible"),
            Error::ReadOnlyTx => write!(f, "Cannot write in a read-only transaction"),
            Error::IOError(e) => write!(f, "IO Error: {}", e),
            Error::SyncError(s) => write!(f, "Sync Error: {}", s),
            Error::InvalidDB(s) => write!(f, "Invalid DB: {}", s),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IOError(err)
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Error {
        Error::SyncError("lock poisoned")
    }
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Error::BucketExists, Error::BucketExists) => true,
            (Error::BucketMissing, Error::BucketMissing) => true,
            (Error::KeyValueMissing, Error::KeyValueMissing) => true,
            (Error::IncompatibleValue, Error::IncompatibleValue) => true,
            (Error::ReadOnlyTx, Error::ReadOnlyTx) => true,
            (Error::IOError(s1), Error::IOError(s2)) => format!("{}", s1) == format!("{}", s2),
            (Error::SyncError(s1), Error::SyncError(s2)) => s1 == s2,
            (Error::InvalidDB(s1), Error::InvalidDB(s2)) => s1 == s2,
            _ => false,
        }
    }
}
