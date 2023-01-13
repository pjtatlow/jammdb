use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    rc::Rc,
};

pub trait ToBytes<'a> {
    fn to_bytes(self) -> Bytes<'a>;
}

impl<'a, T: Into<Bytes<'a>>> ToBytes<'a> for T {
    fn to_bytes(self) -> Bytes<'a> {
        self.into()
    }
}

impl<'a> ToBytes<'a> for &[u8] {
    fn to_bytes(self) -> Bytes<'a> {
        Bytes::Bytes(bytes::Bytes::copy_from_slice(self))
    }
}

impl<'a, const N: usize> ToBytes<'a> for [u8; N] {
    fn to_bytes(self) -> Bytes<'a> {
        Bytes::Bytes(bytes::Bytes::copy_from_slice(&self))
    }
}

#[derive(Debug, Clone)]
pub enum Bytes<'a> {
    Slice(&'a [u8]),
    Bytes(bytes::Bytes),
    Vec(Rc<Vec<u8>>),
    String(Rc<String>),
}

impl<'a> Bytes<'a> {
    pub fn size(&self) -> usize {
        match self {
            Self::Slice(s) => s.len(),
            Self::Bytes(b) => b.len(),
            Self::Vec(v) => v.len(),
            Self::String(s) => s.len(),
        }
    }
}

impl<'a> AsRef<[u8]> for Bytes<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Slice(s) => s,
            Self::Bytes(b) => b,
            Self::Vec(v) => v.as_slice(),
            Self::String(s) => s.as_bytes(),
        }
    }
}

impl<'a> Ord for Bytes<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        let a = self.as_ref();
        let b = other.as_ref();
        a.cmp(b)
    }
}

impl<'a> PartialOrd for Bytes<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> PartialEq for Bytes<'a> {
    fn eq(&self, other: &Self) -> bool {
        let a = self.as_ref();
        let b = other.as_ref();
        a.eq(b)
    }
}

impl<'a> Eq for Bytes<'a> {}

impl<'a> Hash for Bytes<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let a = self.as_ref();
        a.hash(state);
    }
}

impl<'a> From<&'static str> for Bytes<'a> {
    fn from(s: &'static str) -> Self {
        Self::Bytes(bytes::Bytes::from_static(s.as_bytes()))
    }
}

impl<'a> From<String> for Bytes<'a> {
    fn from(s: String) -> Self {
        Self::String(Rc::new(s))
    }
}

impl<'a> From<Vec<u8>> for Bytes<'a> {
    fn from(s: Vec<u8>) -> Self {
        Self::Vec(Rc::new(s))
    }
}

impl<'a> From<bytes::Bytes> for Bytes<'a> {
    fn from(b: bytes::Bytes) -> Self {
        Self::Bytes(b)
    }
}

impl<'a> From<&bytes::Bytes> for Bytes<'a> {
    fn from(b: &bytes::Bytes) -> Self {
        Self::Bytes(b.clone())
    }
}

// impl<'a> From<bytes::BytesMut> for Bytes<'a> {
//     fn from(b: bytes::BytesMut) -> Self {
//         Self::Bytes(b.freeze())
//     }
// }

// impl From<&str> for Bytes {
//     fn from(s: &str) -> Self {
//         Self::Bytes(Bytes::copy_from_slice(s.as_bytes()))
//     }
// }
