use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    rc::Rc,
};

pub trait ToBytes<'a> {
    fn to_bytes(self) -> Bytes<'a>;
}

impl<'a> ToBytes<'a> for &'a [u8] {
    fn to_bytes(self) -> Bytes<'a> {
        Bytes::Slice(self)
    }
}

impl<'a> ToBytes<'a> for &'a str {
    fn to_bytes(self) -> Bytes<'a> {
        Bytes::Slice(self.as_bytes())
    }
}

macro_rules! byte_array_to_bytes {
    ($($n:expr),*) => (
    $(
        impl<'a> ToBytes<'a> for [u8; $n] {
            fn to_bytes(self) -> Bytes<'a> {
                Bytes::Bytes(bytes::Bytes::copy_from_slice(&self))
            }
        }
    )*
)
}

// We don't want to automatically copy arrays of any length,
// but for concenience, we'll copy arrays for integer sizes
// so that if you do i.to_be_bytes() it will work for any int.
byte_array_to_bytes!(1, 2, 4, 8, 16);

impl<'a> ToBytes<'a> for String {
    fn to_bytes(self) -> Bytes<'a> {
        Bytes::String(Rc::new(self))
    }
}

impl<'a> ToBytes<'a> for Vec<u8> {
    fn to_bytes(self) -> Bytes<'a> {
        Bytes::Vec(Rc::new(self))
    }
}

impl<'a> ToBytes<'a> for bytes::Bytes {
    fn to_bytes(self) -> Bytes<'a> {
        Bytes::Bytes(self)
    }
}

impl<'a> ToBytes<'a> for &bytes::Bytes {
    fn to_bytes(self) -> Bytes<'a> {
        Bytes::Bytes(self.clone())
    }
}

impl<'a> ToBytes<'a> for Bytes<'a> {
    fn to_bytes(self) -> Bytes<'a> {
        self
    }
}

impl<'a> ToBytes<'a> for &Bytes<'a> {
    fn to_bytes(self) -> Bytes<'a> {
        self.clone()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_vec() {
        let vec: Vec<u8> = vec![0, 0, 0];
        let ptr = vec.as_slice()[0] as *const u8;
        let b: Bytes = vec.to_bytes();
        let ptr2 = b.as_ref()[0] as *const u8;
        assert!(ptr == ptr2);
    }

    #[test]
    fn from_str() {
        let s = "abc";
        let ptr = s.as_bytes()[0] as *const u8;
        let b: Bytes = s.to_bytes();
        let ptr2 = b.as_ref()[0] as *const u8;
        assert!(ptr == ptr2);
    }
}
