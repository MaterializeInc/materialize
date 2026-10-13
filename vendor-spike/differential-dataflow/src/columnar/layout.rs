//! Layout traits for columnar arrangements.
//!
//! `ColumnarUpdate` names the four constituent columnar types of an update,
//! and `ColumnarLayout` glues them into a DD `Layout` backed by `Coltainer`.

use std::fmt::Debug;
use columnar::Columnar;
use crate::trace::implementations::{Layout, OffsetList};
use crate::difference::Semigroup;
use crate::lattice::Lattice;
use timely::progress::Timestamp;

/// A layout based on columnar
pub struct ColumnarLayout<U: ColumnarUpdate> {
    phantom: std::marker::PhantomData<U>,
}

impl<K, V, T, R> ColumnarUpdate for (K, V, T, R)
where
    K: Columnar<Container: OrdContainer + Default> + Debug + Ord + Clone + 'static,
    V: Columnar<Container: OrdContainer + Default> + Debug + Ord + Clone + 'static,
    T: Columnar<Container: OrdContainer + Default> + Debug + Ord + Default + Clone + Lattice + Timestamp,
    R: Columnar<Container: OrdContainer + Default> + Debug + Ord + Default + Semigroup + 'static,
{
    type Key = K;
    type Val = V;
    type Time = T;
    type Diff = R;
}

impl<U: ColumnarUpdate> Layout for ColumnarLayout<U> {
    type KeyContainer    = Coltainer<U::Key>;
    type ValContainer    = Coltainer<U::Val>;
    type TimeContainer   = Coltainer<U::Time>;
    type DiffContainer   = Coltainer<U::Diff>;
    type OffsetContainer = OffsetList;
}

/// A type that names constituent update types.
///
/// We will use their associated `Columnar::Container`
pub trait ColumnarUpdate : Debug + 'static {
    /// The key type.
    type Key:  Columnar<Container: OrdContainer + Default> + Debug + Ord + Clone + 'static;
    /// The value type.
    type Val:  Columnar<Container: OrdContainer + Default> + Debug + Ord + Clone + 'static;
    /// The time type.
    type Time: Columnar<Container: OrdContainer + Default> + Debug + Ord + Default + Clone + Lattice + Timestamp;
    /// The difference type.
    type Diff: Columnar<Container: OrdContainer + Default> + Debug + Ord + Default + Semigroup + 'static;
}

/// A container whose references can be ordered.
pub trait OrdContainer : for<'a> columnar::Container<Ref<'a> : Ord> { }
impl<C: for<'a> columnar::Container<Ref<'a> : Ord>> OrdContainer for C { }

pub use batch_container::Coltainer;
mod batch_container {
    //! [`Coltainer`] wraps a columnar container as a DD [`BatchContainer`].

    use columnar::{Borrow, Columnar, Container, Clear, Push, Index, Len};
    use crate::trace::implementations::BatchContainer;

    /// Container, anchored by `C` to provide an owned type.
    pub struct Coltainer<C: Columnar> {
        /// The underlying columnar container.
        pub container: C::Container,
    }

    impl<C: Columnar> Default for Coltainer<C> {
        fn default() -> Self { Self { container: Default::default() } }
    }

    impl<C: Columnar + Ord + Clone> BatchContainer for Coltainer<C> where for<'a> columnar::Ref<'a, C> : Ord {

        type ReadItem<'a> = columnar::Ref<'a, C>;
        type Owned = C;

        #[inline(always)] fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned { C::into_owned(item) }
        #[inline(always)] fn clone_onto<'a>(item: Self::ReadItem<'a>, other: &mut Self::Owned) { other.copy_from(item) }

        #[inline(always)] fn push_ref(&mut self, item: Self::ReadItem<'_>) { self.container.push(item) }
        #[inline(always)] fn push_own(&mut self, item: &Self::Owned) { self.container.push(item) }

        /// Clears the container. May not release resources.
        fn clear(&mut self) { self.container.clear() }

        /// Creates a new container with sufficient capacity.
        fn with_capacity(_size: usize) -> Self { Self::default() }
        /// Creates a new container with sufficient capacity.
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            Self {
                container: <C as Columnar>::Container::with_capacity_for([cont1.container.borrow(), cont2.container.borrow()].into_iter()),
            }
         }

        /// Converts a read item into one with a narrower lifetime.
        #[inline(always)] fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> { columnar::ContainerOf::<C>::reborrow_ref(item) }

        /// Reference to the element at this position.
        #[inline(always)] fn index(&self, index: usize) -> Self::ReadItem<'_> { self.container.borrow().get(index) }

        #[inline(always)] fn len(&self) -> usize { self.container.len() }

        /// Reports the number of elements satisfying the predicate.
        ///
        /// This methods *relies strongly* on the assumption that the predicate
        /// stays false once it becomes false, a joint property of the predicate
        /// and the layout of `Self. This allows `advance` to use exponential search to
        /// count the number of elements in time logarithmic in the result.
        fn advance<F: for<'a> Fn(Self::ReadItem<'a>)->bool>(&self, start: usize, end: usize, function: F) -> usize {

            let borrow = self.container.borrow();

            let small_limit = 8;

            // Exponential search if the answer isn't within `small_limit`.
            if end > start + small_limit && function(borrow.get(start + small_limit)) {

                // start with no advance
                let mut index = small_limit + 1;
                if start + index < end && function(borrow.get(start + index)) {

                    // advance in exponentially growing steps.
                    let mut step = 1;
                    while start + index + step < end && function(borrow.get(start + index + step)) {
                        index += step;
                        step <<= 1;
                    }

                    // advance in exponentially shrinking steps.
                    step >>= 1;
                    while step > 0 {
                        if start + index + step < end && function(borrow.get(start + index + step)) {
                            index += step;
                        }
                        step >>= 1;
                    }

                    index += 1;
                }

                index
            }
            else {
                let limit = std::cmp::min(end, start + small_limit);
                (start .. limit).filter(|x| function(borrow.get(*x))).count()
            }
        }
    }
}
