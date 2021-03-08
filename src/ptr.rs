use std::ops::{Deref, DerefMut};

#[derive(Clone)]
pub struct Ptr<T>(pub(crate) *const T);

impl<T> Deref for Ptr<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.0 }
    }
}

impl<T> DerefMut for Ptr<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *(self.0 as *mut T) }
    }
}

impl<T> Ptr<T> {
    pub(crate) fn new(a: &T) -> Ptr<T> {
        Ptr(a as *const T)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ptr() {
        let val = 0_u8;
        let mut ptr = Ptr::new(&val);

        assert_eq!(*(ptr.deref()), 0);
        *ptr.deref_mut() = 8;
        assert_eq!(*(ptr.deref()), 8);
    }
}
