use std::ops::{Deref, DerefMut};

#[derive(Clone)]
pub (crate) struct Ptr<T> (pub (crate) *const T);

impl<T> Deref for Ptr<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
		// println!("{:?}", self.0);
        unsafe{&*self.0}
    }
}

impl<T> DerefMut for Ptr<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
		unsafe{&mut *(self.0 as *mut T)}
    }
}

impl<T> Ptr<T> {
	pub (crate) fn new(a: &T) -> Ptr<T> {
		Ptr(a as *const T)
	}
}
