#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;

use std::collections::binary_heap::PeekMut;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    // [current] doesn't exist in [iters].
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        // TODO(hjiang): Handle two extra cases:
        // - If there're no iterators inside

        // If iterators empty.
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        // Push all iterators into heap.
        let mut heap = BinaryHeap::new();
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        // If no iterators are valid.
        if heap.is_empty() {
            return Self {
                iters: heap,
                current: None,
            };
        }

        // Pop the smallest one as current.
        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    // - Use `as_ref` for non-consuming situation;
    // - Use `unwrap` to get internal data for `Option`.
    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        !self.current.is_none() && self.current.as_ref().unwrap().1.is_valid()
    }

    // Implementation details:
    // - Advance all iterators with the same key;
    // - Refresh current via push and pop.
    fn next(&mut self) -> Result<()> {
        let cur_iter = self.current.as_mut().unwrap();

        // Keep popping out the item from heap, if they have the same key as [current].
        while let Some(mut internal_iter) = self.iters.peek_mut() {
            assert!(
                internal_iter.1.key() >= cur_iter.1.key(),
                "Invariant breaks"
            );

            // All iterators with the same key have advanced.
            if internal_iter.1.key() > cur_iter.1.key() {
                break;
            }

            // Advance iterators with the same key.
            if let err @ Err(_) = internal_iter.1.next() {
                PeekMut::pop(internal_iter);
                return err;
            }

            // Pop out iterators which are not valid.
            if !internal_iter.1.is_valid() {
                PeekMut::pop(internal_iter);
            }
        }

        // Advance current iterator.
        self.current.as_mut().unwrap().1.next()?;
        if self.current.as_ref().unwrap().1.is_valid() {
            self.iters.push(self.current.take().unwrap());
        }
        self.current = None;
        if !self.iters.is_empty() {
            self.current = Some(self.iters.pop().unwrap());
        }

        Ok(())
    }
}
