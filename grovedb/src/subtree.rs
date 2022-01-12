//! Module for subtrees handling.
//! Subtrees handling is isolated so basically this module is about adapting
//! Merk API to GroveDB needs.
use std::collections::HashMap;
use std::ops::{Range, RangeFrom, RangeTo, RangeToInclusive};

use merk::{
    proofs::{query::QueryItem, Query},
    tree::Tree,
    Op,
};
use serde::{Deserialize, Serialize};
use proc_macro::tracked_path::path;
use storage::{
    rocksdb_storage::{
        OptimisticTransactionDBTransaction, PrefixedRocksDbStorage,
        RawPrefixedTransactionalIterator,
    },
    RawIterator, Storage, Store,
};

use crate::{Error, GroveDb, Merk, PathQuery, SizedQuery};

/// Variants of GroveDB stored entities
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Element {
    /// An ordinary value
    Item(Vec<u8>),
    /// A reference to an object by its path
    Reference(Vec<Vec<u8>>),
    /// A subtree, contains a root hash of the underlying Merk.
    /// Hash is stored to make Merk become different when its subtrees have
    /// changed, otherwise changes won't be reflected in parent trees.
    Tree([u8; 32]),
}

impl Element {
    // TODO: improve API to avoid creation of Tree elements with uncertain state
    pub fn empty_tree() -> Element {
        Element::Tree(Default::default())
    }

    /// Delete an element from Merk under a key
    pub fn delete(
        merk: &mut Merk<PrefixedRocksDbStorage>,
        key: Vec<u8>,
        transaction: Option<&OptimisticTransactionDBTransaction>,
    ) -> Result<(), Error> {
        // TODO: delete references on this element
        let batch = [(key, Op::Delete)];
        merk.apply(&batch, &[], transaction)
            .map_err(|e| Error::CorruptedData(e.to_string()))
    }

    /// Get an element from Merk under a key; path should be resolved and proper
    /// Merk should be loaded by this moment
    pub fn get(merk: &Merk<PrefixedRocksDbStorage>, key: &[u8]) -> Result<Element, Error> {
        let element = bincode::deserialize(
            merk.get(&key)
                .map_err(|e| Error::CorruptedData(e.to_string()))?
                .ok_or(Error::InvalidPath("key not found in Merk"))?
                .as_slice(),
        )
        .map_err(|_| Error::CorruptedData(String::from("unable to deserialize element")))?;
        Ok(element)
    }

    pub fn get_query(
        merk: &Merk<PrefixedRocksDbStorage>,
        query: &Query,
    ) -> Result<Vec<Element>, Error> {
        let sized_query = SizedQuery::new(query.clone(), None, None, true);
        let (elements, skipped) = Element::get_sized_query(merk, &sized_query)?;
        Ok(elements)
    }

    fn basic_push(_subtrees: Option<&HashMap<Vec<u8>, Merk<PrefixedRocksDbStorage>>>, _key: Option<&[u8]>, element: Element, _path: Option<&[&[u8]]>, results: &mut Vec<Element>, mut limit: u16, mut offset: u16) -> Result<(), Error> {
        if offset == 0 {
            results.push(element);
            limit -= 1;
        } else {
            offset -= 1;
        }
        Ok(())
    }

    fn path_query_push(subtrees_option: Option<&HashMap<Vec<u8>, Merk<PrefixedRocksDbStorage>>>, key: Option<&[u8]>, element: Element, path: Option<&[&[u8]]>, results: &mut Vec<Element>, mut limit: u16, mut offset: u16) -> Result<(), Error> {
        match element {
            Element::Tree(_) => {
                // if the query had a subquery then we should get elements from it
                if let Some(subquery_key) = path_query.subquery_key {
                    let subtrees = subtrees_option.ok_or(Error::MissingParameter(
                        "subtrees must be provided when using a subquery key",
                    ))?;
                    // this means that for each element we should get the element at
                    // the subquery_key
                    let mut path_vec = path.to_vec();
                    path_vec.push(key);

                    if path_query.subquery.is_some() {
                        path_vec.push(subquery_key);

                        let inner_merk = subtrees
                            .get(&Self::compress_subtree_key(
                                path_vec.as_slice(),
                                None,
                            ))
                            .ok_or(Error::InvalidPath(
                                "no subtree found under that path",
                            ))?;
                        let inner_limit = if sized_query.limit.is_some() {
                            Some(limit)
                        } else {
                            None
                        };
                        let inner_offset = if sized_query.offset.is_some() {
                            Some(offset)
                        } else {
                            None
                        };
                        let inner_query = SizedQuery::new(
                            path_query.subquery.clone().unwrap(),
                            inner_limit,
                            inner_offset,
                            sized_query.left_to_right,
                        );
                        let (mut sub_elements, skipped) =
                            Element::get_sized_query(inner_merk, &inner_query)?;
                        limit -= sub_elements.len() as u16;
                        offset -= skipped;
                        results.append(&mut sub_elements);
                    } else {
                        let inner_merk = subtrees
                            .get(&Self::compress_subtree_key(
                                path_vec.as_slice(),
                                None,
                            ))
                            .ok_or(Error::InvalidPath(
                                "no subtree found under that path",
                            ))?;
                        if offset == 0 {
                            results.push(Element::get(inner_merk, subquery_key)?);
                            limit -= 1;
                        } else {
                            offset -= 1;
                        }
                    }
                }
            }
            _ => {
                if offset == 0 {
                    results.push(element);
                    limit -= 1;
                } else {
                    offset -= 1;
                }
            }
        }
        Ok(())
    }

    // Returns a vector of elements, and the number of skipped elements
    pub fn get_sized_query(
        merk: &Merk<PrefixedRocksDbStorage>,
        sized_query: &SizedQuery,
    ) -> Result<(Vec<Element>, u16), Error> {
        Element::get_query_apply_function(merk, sized_query, None, None, Element::basic_push)
    }

    pub fn get_query_apply_function(
        merk: &Merk<PrefixedRocksDbStorage>,
        sized_query: &SizedQuery,
        path: Option<&[&[u8]]>,
        subtrees: Option<&HashMap<Vec<u8>, Merk<PrefixedRocksDbStorage>>>,
        add_element_function: fn(subtrees: Option<&HashMap<Vec<u8>, Merk<PrefixedRocksDbStorage>>>,
                                 key: Option<&[u8]>,
                                 element: Element,
                                 path: Option<&[&[u8]]>,
                                 &mut Vec<Element>,
                                 mut limit: u16,
                                 mut offset: u16) -> Result<(), Error>
    ) -> Result<(Vec<Element>, u16), Error> {
        let mut results = Vec::new();
        let mut iter = merk.raw_iter();

        let mut limit = if sized_query.limit.is_some() {
            sized_query.limit.unwrap()
        } else {
            u16::MAX
        };
        let original_offset = if sized_query.offset.is_some() {
            sized_query.offset.unwrap()
        } else {
            0 as u16
        };
        let mut offset = original_offset;

        for item in sized_query.query.iter() {
            if !item.is_range() {
                // this is a query on a key
                if let QueryItem::Key(key) = item {
                    add_element_function(subtrees, Some(key.as_slice()), Element::get(merk, key)?, Some(), &mut results, limit, offset);
                }
            } else {
                // this is a query on a range
                item.seek_for_iter(&mut iter, sized_query.left_to_right);
                let mut work = true;

                loop {
                    let (valid, next_valid) =
                        item.iter_is_valid_for_type(&iter, limit, work, sized_query.left_to_right);
                    if !valid {
                        break;
                    }
                    work = next_valid;
                    let element =
                        raw_decode(iter.value().expect("if key exists then value should too"))?;
                    let key = iter.key().expect("key should exist")?;
                    add_element_function(subtrees, key, element, path, &mut results, limit, offset);
                    if sized_query.left_to_right {
                        iter.next();
                    } else {
                        iter.prev();
                    }
                }
            }
            if limit == 0 {
                break;
            }
        }
        Ok((result, original_offset - offset))
    }

    // Returns a vector of elements, and the number of skipped elements
    pub fn get_path_query(
        merk: &Merk<PrefixedRocksDbStorage>,
        path_query: &PathQuery,
        subtrees: Option<&HashMap<Vec<u8>, Merk<PrefixedRocksDbStorage>>>,
    ) -> Result<(Vec<Element>, u16), Error> {
        Element::get_query_apply_function(merk, &path_query.query, Some(path_query.path), subtrees, Element::path_query_push)
        let path = ;
        let mut result = Vec::new();
        let mut iter = merk.raw_iter();

        let mut limit = if path_query.query.limit.is_some() {
            sized_query.limit.unwrap()
        } else {
            u16::MAX
        };
        let original_offset = if path_query.query.offset.is_some() {
            sized_query.offset.unwrap()
        } else {
            0 as u16
        };
        let mut offset = original_offset;

        for item in path_query.query.query.iter() {
            if !item.is_range() {
                // this is a query on a key
                if let QueryItem::Key(key) = item {
                    if limit > 0 {
                        if offset == 0 {
                            result.push(Element::get(merk, key)?);
                            limit -= 1;
                        } else {
                            offset -= 1;
                        }
                    }
                }
            } else {
                // this is a query on a range

                item.seek_for_iter(&mut iter, sized_query.left_to_right);
                let mut work = true;

                loop {
                    let (valid, next_valid) =
                        item.iter_is_valid_for_type(&iter, limit, work, sized_query.left_to_right);
                    if !valid {
                        break;
                    }
                    work = next_valid;
                    let element =
                        raw_decode(iter.value().expect("if key exists then value should too"))?;

                    if offset == 0 {
                        result.push(element);
                        limit -= 1;
                    } else {
                        offset -= 1;
                    }
                    if sized_query.left_to_right {
                        iter.next();
                    } else {
                        iter.prev();
                    }
                }
            }
            if limit == 0 {
                break;
            }
        }
        Ok((result, original_offset - offset))
    }

    /// Insert an element in Merk under a key; path should be resolved and
    /// proper Merk should be loaded by this moment
    /// If transaction is not passed, the batch will be written immediately.
    /// If transaction is passed, the operation will be committed on the
    /// transaction commit.
    pub fn insert<'a: 'b, 'b>(
        &'a self,
        merk: &mut Merk<PrefixedRocksDbStorage>,
        key: Vec<u8>,
        transaction: Option<&'b <PrefixedRocksDbStorage as Storage>::DBTransaction<'b>>,
    ) -> Result<(), Error> {
        let batch_operations =
            [(
                key,
                Op::Put(bincode::serialize(self).map_err(|_| {
                    Error::CorruptedData(String::from("unable to serialize element"))
                })?),
            )];
        merk.apply(&batch_operations, &[], transaction)
            .map_err(|e| Error::CorruptedData(e.to_string()))
    }

    pub fn iterator(mut raw_iter: RawPrefixedTransactionalIterator) -> ElementsIterator {
        raw_iter.seek_to_first();
        ElementsIterator { raw_iter }
    }
}

pub struct ElementsIterator<'a> {
    raw_iter: RawPrefixedTransactionalIterator<'a>,
}

pub fn raw_decode(bytes: &[u8]) -> Result<Element, Error> {
    let tree = <Tree as Store>::decode(bytes).map_err(|e| Error::CorruptedData(e.to_string()))?;
    let element: Element = bincode::deserialize(tree.value())
        .map_err(|_| Error::CorruptedData(String::from("unable to deserialize element")))?;
    Ok(element)
}

impl<'a> ElementsIterator<'a> {
    pub fn next(&mut self) -> Result<Option<(Vec<u8>, Element)>, Error> {
        Ok(if self.raw_iter.valid() {
            if let Some((key, value)) = self.raw_iter.key().zip(self.raw_iter.value()) {
                let element = raw_decode(value)?;
                let key = key.to_vec();
                self.raw_iter.next();
                Some((key, element))
            } else {
                None
            }
        } else {
            None
        })
    }
}

#[cfg(test)]
mod tests {
    use merk::test_utils::TempMerk;

    use super::*;

    #[test]
    fn test_success_insert() {
        let mut merk = TempMerk::new();
        Element::empty_tree()
            .insert(&mut merk, b"mykey".to_vec(), None)
            .expect("expected successful insertion");
        Element::Item(b"value".to_vec())
            .insert(&mut merk, b"another-key".to_vec(), None)
            .expect("expected successful insertion 2");

        assert_eq!(
            Element::get(&merk, b"another-key").expect("expected successful get"),
            Element::Item(b"value".to_vec()),
        );
    }

    #[test]
    fn test_get_query() {
        let mut merk = TempMerk::new();
        Element::Item(b"ayyd".to_vec())
            .insert(&mut merk, b"d".to_vec(), None)
            .expect("expected successful insertion");
        Element::Item(b"ayyc".to_vec())
            .insert(&mut merk, b"c".to_vec(), None)
            .expect("expected successful insertion");
        Element::Item(b"ayya".to_vec())
            .insert(&mut merk, b"a".to_vec(), None)
            .expect("expected successful insertion");
        Element::Item(b"ayyb".to_vec())
            .insert(&mut merk, b"b".to_vec(), None)
            .expect("expected successful insertion");

        // Test queries by key
        let mut query = Query::new();
        query.insert_key(b"c".to_vec());
        query.insert_key(b"a".to_vec());
        assert_eq!(
            Element::get_query(&mut merk, &query).expect("expected successful get_query"),
            vec![
                Element::Item(b"ayya".to_vec()),
                Element::Item(b"ayyc".to_vec())
            ]
        );

        // Test range query
        let mut query = Query::new();
        query.insert_range(b"b".to_vec()..b"d".to_vec());
        query.insert_range(b"a".to_vec()..b"c".to_vec());
        assert_eq!(
            Element::get_query(&mut merk, &query).expect("expected successful get_query"),
            vec![
                Element::Item(b"ayya".to_vec()),
                Element::Item(b"ayyb".to_vec()),
                Element::Item(b"ayyc".to_vec())
            ]
        );

        // Test range inclusive query
        let mut query = Query::new();
        query.insert_range_inclusive(b"b".to_vec()..=b"d".to_vec());
        query.insert_range(b"b".to_vec()..b"c".to_vec());
        assert_eq!(
            Element::get_query(&mut merk, &query).expect("expected successful get_query"),
            vec![
                Element::Item(b"ayyb".to_vec()),
                Element::Item(b"ayyc".to_vec()),
                Element::Item(b"ayyd".to_vec())
            ]
        );

        // Test overlaps
        let mut query = Query::new();
        query.insert_key(b"a".to_vec());
        query.insert_range(b"b".to_vec()..b"d".to_vec());
        query.insert_range(b"a".to_vec()..b"c".to_vec());
        assert_eq!(
            Element::get_query(&mut merk, &query).expect("expected successful get_query"),
            vec![
                Element::Item(b"ayya".to_vec()),
                Element::Item(b"ayyb".to_vec()),
                Element::Item(b"ayyc".to_vec())
            ]
        );
    }

    #[test]
    fn test_get_range_query() {
        let mut merk = TempMerk::new();
        Element::Item(b"ayyd".to_vec())
            .insert(&mut merk, b"d".to_vec(), None)
            .expect("expected successful insertion");
        Element::Item(b"ayyc".to_vec())
            .insert(&mut merk, b"c".to_vec(), None)
            .expect("expected successful insertion");
        Element::Item(b"ayya".to_vec())
            .insert(&mut merk, b"a".to_vec(), None)
            .expect("expected successful insertion");
        Element::Item(b"ayyb".to_vec())
            .insert(&mut merk, b"b".to_vec(), None)
            .expect("expected successful insertion");

        // Test range inclusive query
        let mut query = Query::new();
        query.insert_range(b"a".to_vec()..b"d".to_vec());

        let ascending_query = SizedQuery::new(query.clone(), None, None, true);
        let (elements, skipped) = Element::get_sized_query(&mut merk, &ascending_query)
            .expect("expected successful get_query");
        assert_eq!(
            elements,
            vec![
                Element::Item(b"ayya".to_vec()),
                Element::Item(b"ayyb".to_vec()),
                Element::Item(b"ayyc".to_vec()),
            ]
        );
        assert_eq!(skipped, 0);

        let backwards_query = SizedQuery::new(query.clone(), None, None, false);
        let (elements, skipped) = Element::get_sized_query(&mut merk, &backwards_query)
            .expect("expected successful get_query");
        assert_eq!(
            elements,
            vec![
                Element::Item(b"ayyc".to_vec()),
                Element::Item(b"ayyb".to_vec()),
                Element::Item(b"ayya".to_vec()),
            ]
        );
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_get_range_inclusive_query() {
        let mut merk = TempMerk::new();
        Element::Item(b"ayyd".to_vec())
            .insert(&mut merk, b"d".to_vec(), None)
            .expect("expected successful insertion");
        Element::Item(b"ayyc".to_vec())
            .insert(&mut merk, b"c".to_vec(), None)
            .expect("expected successful insertion");
        Element::Item(b"ayya".to_vec())
            .insert(&mut merk, b"a".to_vec(), None)
            .expect("expected successful insertion");
        Element::Item(b"ayyb".to_vec())
            .insert(&mut merk, b"b".to_vec(), None)
            .expect("expected successful insertion");

        // Test range inclusive query
        let mut query = Query::new();
        query.insert_range_inclusive(b"a".to_vec()..=b"d".to_vec());

        let ascending_query = SizedQuery::new(query.clone(), None, None, true);
        let (elements, skipped) = Element::get_sized_query(&mut merk, &ascending_query)
            .expect("expected successful get_query");
        assert_eq!(
            elements,
            vec![
                Element::Item(b"ayya".to_vec()),
                Element::Item(b"ayyb".to_vec()),
                Element::Item(b"ayyc".to_vec()),
                Element::Item(b"ayyd".to_vec()),
            ]
        );
        assert_eq!(skipped, 0);

        let backwards_query = SizedQuery::new(query.clone(), None, None, false);
        let (elements, skipped) = Element::get_sized_query(&mut merk, &backwards_query)
            .expect("expected successful get_query");
        assert_eq!(
            elements,
            vec![
                Element::Item(b"ayyd".to_vec()),
                Element::Item(b"ayyc".to_vec()),
                Element::Item(b"ayyb".to_vec()),
                Element::Item(b"ayya".to_vec()),
            ]
        );
        assert_eq!(skipped, 0);

        // Test range inclusive query
        let mut query = Query::new();
        query.insert_range_inclusive(b"b".to_vec()..=b"d".to_vec());
        query.insert_range(b"a".to_vec()..b"c".to_vec());

        let backwards_query = SizedQuery::new(query.clone(), None, None, false);
        let (elements, skipped) = Element::get_sized_query(&mut merk, &backwards_query)
            .expect("expected successful get_query");
        assert_eq!(
            elements,
            vec![
                Element::Item(b"ayyd".to_vec()),
                Element::Item(b"ayyc".to_vec()),
                Element::Item(b"ayyb".to_vec()),
                Element::Item(b"ayya".to_vec()),
            ]
        );
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_get_limit_query() {
        let mut merk = TempMerk::new();
        Element::Item(b"ayyd".to_vec())
            .insert(&mut merk, b"d".to_vec(), None)
            .expect("expected successful insertion");
        Element::Item(b"ayyc".to_vec())
            .insert(&mut merk, b"c".to_vec(), None)
            .expect("expected successful insertion");
        Element::Item(b"ayya".to_vec())
            .insert(&mut merk, b"a".to_vec(), None)
            .expect("expected successful insertion");
        Element::Item(b"ayyb".to_vec())
            .insert(&mut merk, b"b".to_vec(), None)
            .expect("expected successful insertion");

        // Test queries by key
        let mut query = Query::new();
        query.insert_key(b"c".to_vec());
        query.insert_key(b"a".to_vec());

        // since these are just keys a backwards query will keep same order
        let backwards_query = SizedQuery::new(query.clone(), None, None, false);
        let (elements, skipped) = Element::get_sized_query(&mut merk, &backwards_query)
            .expect("expected successful get_query");
        assert_eq!(
            elements,
            vec![
                Element::Item(b"ayya".to_vec()),
                Element::Item(b"ayyc".to_vec()),
            ]
        );
        assert_eq!(skipped, 0);

        // The limit will mean we will only get back 1 item
        let limit_query = SizedQuery::new(query.clone(), Some(1), None, false);
        let (elements, skipped) = Element::get_sized_query(&mut merk, &limit_query)
            .expect("expected successful get_query");
        assert_eq!(elements, vec![Element::Item(b"ayya".to_vec()),]);
        assert_eq!(skipped, 0);

        // Test range query
        let mut query = Query::new();
        query.insert_range(b"b".to_vec()..b"d".to_vec());
        query.insert_range(b"a".to_vec()..b"c".to_vec());
        let limit_query = SizedQuery::new(query.clone(), Some(2), None, true);
        let (elements, skipped) = Element::get_sized_query(&mut merk, &limit_query)
            .expect("expected successful get_query");
        assert_eq!(
            elements,
            vec![
                Element::Item(b"ayya".to_vec()),
                Element::Item(b"ayyb".to_vec())
            ]
        );
        assert_eq!(skipped, 0);

        let limit_offset_query = SizedQuery::new(query.clone(), Some(2), Some(1), true);
        let (elements, skipped) = Element::get_sized_query(&mut merk, &limit_offset_query)
            .expect("expected successful get_query");
        assert_eq!(
            elements,
            vec![
                Element::Item(b"ayyb".to_vec()),
                Element::Item(b"ayyc".to_vec())
            ]
        );
        assert_eq!(skipped, 1);

        let limit_offset_backwards_query = SizedQuery::new(query.clone(), Some(2), Some(1), false);
        let (elements, skipped) =
            Element::get_sized_query(&mut merk, &limit_offset_backwards_query)
                .expect("expected successful get_query");
        assert_eq!(
            elements,
            vec![
                Element::Item(b"ayyb".to_vec()),
                Element::Item(b"ayya".to_vec())
            ]
        );
        assert_eq!(skipped, 1);

        // Test range inclusive query
        let mut query = Query::new();
        query.insert_range_inclusive(b"b".to_vec()..=b"d".to_vec());
        query.insert_range(b"b".to_vec()..b"c".to_vec());
        let limit_full_query = SizedQuery::new(query.clone(), Some(5), Some(0), true);
        let (elements, skipped) = Element::get_sized_query(&mut merk, &limit_full_query)
            .expect("expected successful get_query");
        assert_eq!(
            elements,
            vec![
                Element::Item(b"ayyb".to_vec()),
                Element::Item(b"ayyc".to_vec()),
                Element::Item(b"ayyd".to_vec()),
            ]
        );
        assert_eq!(skipped, 0);
        let limit_offset_backwards_query = SizedQuery::new(query.clone(), Some(2), Some(1), false);
        let (elements, skipped) =
            Element::get_sized_query(&mut merk, &limit_offset_backwards_query)
                .expect("expected successful get_query");
        assert_eq!(
            elements,
            vec![
                Element::Item(b"ayyc".to_vec()),
                Element::Item(b"ayyb".to_vec()),
            ]
        );
        assert_eq!(skipped, 1);

        // Test overlaps
        let mut query = Query::new();
        query.insert_key(b"a".to_vec());
        query.insert_range(b"b".to_vec()..b"d".to_vec());
        query.insert_range(b"b".to_vec()..b"c".to_vec());
        let limit_backwards_query = SizedQuery::new(query.clone(), Some(2), None, false);
        let (elements, skipped) = Element::get_sized_query(&mut merk, &limit_backwards_query)
            .expect("expected successful get_query");
        assert_eq!(
            elements,
            vec![
                Element::Item(b"ayya".to_vec()),
                Element::Item(b"ayyc".to_vec()),
            ]
        );
        assert_eq!(skipped, 0);
    }
}
