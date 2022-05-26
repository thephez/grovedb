use storage::StorageContext;

use crate::{
    util::{merk_optional_tx, storage_context_optional_tx},
    Element, Error, GroveDb, TransactionArg,
};

impl GroveDb {
    /// Deletes up a tree while the deletion of an element causes the parent tree to be empty
    ///
    /// # Arguments
    ///
    /// * `path` - The path at which we want to delete the item
    /// * `key` - The key of the item we wish to delete
    /// * `stop_path_height` - At what tree depth do we want to want to stop deleting if empty?
    /// * `transaction` - The database transaction
    ///
    /// # Returns
    /// * u16: number of deleted elements
    /// * usize: size removed from disk
    /// * u16: key seeks
    /// * usize: key size loaded into memory

    pub fn delete_up_tree_while_empty<'p, P>(
        &self,
        path: P,
        key: &'p [u8],
        stop_path_height: Option<u16>,
        transaction: TransactionArg,
    ) -> Result<(u16, usize, u16, usize), Error>
    where
        P: IntoIterator<Item = &'p [u8]>,
        <P as IntoIterator>::IntoIter: DoubleEndedIterator + ExactSizeIterator + Clone,
    {
        let mut key_seeks : u16 = 0;
        let mut key_size_loaded : usize = 0;
        let mut path_iter = path.into_iter();

        //todo: Not sure we actually need this
        self.check_subtree_exists_path_not_found(path_iter.clone(), Some(key), transaction)?;
        key_seeks += 1;
        key_size_loaded += key.len();

        if let Some(stop_path_height) = stop_path_height {
            if stop_path_height == path_iter.clone().len() as u16 {
                return Ok((0, 0, key_seeks, key_size_loaded));
            }
        }

        key_seeks += 1;
        key_size_loaded += key.len();
        if let Some(mut deleted_size) =
            self.delete_internal(path_iter.clone(), key, true, transaction)?
        {
            let mut delete_count: u16 = 1;
            if let Some(last) = path_iter.next_back() {
                let (deleted_parent, deleted_parent_size, parent_key_seeks, parent_key_size_loaded) = self.delete_up_tree_while_empty(
                    path_iter,
                    last,
                    stop_path_height,
                    transaction,
                )?;
                delete_count += deleted_parent;
                deleted_size += deleted_parent_size;
                key_seeks += parent_key_seeks;
                key_size_loaded += parent_key_size_loaded;
            }
            Ok((delete_count, deleted_size, key_seeks, key_size_loaded))
        } else {
            // We didn't delete because the tree wasn't empty and we said to only delete if tree was empty
            Ok((0, 0, key_seeks, key_size_loaded))
        }
    }

    pub fn delete<'p, P>(
        &self,
        path: P,
        key: &'p [u8],
        transaction: TransactionArg,
    ) -> Result<Option<usize>, Error>
    where
        P: IntoIterator<Item = &'p [u8]>,
        <P as IntoIterator>::IntoIter: DoubleEndedIterator + ExactSizeIterator + Clone,
    {
        self.delete_internal(path, key, false, transaction)
    }

    pub fn delete_if_empty_tree<'p, P>(
        &self,
        path: P,
        key: &'p [u8],
        transaction: TransactionArg,
    ) -> Result<Option<usize>, Error>
    where
        P: IntoIterator<Item = &'p [u8]>,
        <P as IntoIterator>::IntoIter: DoubleEndedIterator + ExactSizeIterator + Clone,
    {
        self.delete_internal(path, key, true, transaction)
    }

    // Returns the amount of freed up space
    fn delete_internal<'p, P>(
        &self,
        path: P,
        key: &'p [u8],
        only_delete_tree_if_empty: bool,
        transaction: TransactionArg,
    ) -> Result<Option<usize>, Error>
    where
        P: IntoIterator<Item = &'p [u8]>,
        <P as IntoIterator>::IntoIter: DoubleEndedIterator + ExactSizeIterator + Clone,
    {
        let path_iter = path.into_iter();
        if path_iter.len() == 0 {
            // Attempt to delete a root tree leaf
            Err(Error::InvalidPath(
                "root tree leaves currently cannot be deleted",
            ))
        } else {
            self.check_subtree_exists_path_not_found(path_iter.clone(), Some(key), transaction)?;
            let element = self.get_raw(path_iter.clone(), key.as_ref(), transaction)?;
            let delete_element = || -> Result<(), Error> {
                merk_optional_tx!(self.db, path_iter.clone(), transaction, mut parent_merk, {
                    Element::delete(&mut parent_merk, &key)?;
                    Ok(())
                })
            };

            if let Element::Tree(_) = element {
                let subtree_merk_path = path_iter.clone().chain(std::iter::once(key));
                let subtrees_paths = self.find_subtrees(subtree_merk_path.clone(), transaction)?;
                let is_empty =
                    merk_optional_tx!(self.db, subtree_merk_path, transaction, subtree, {
                        subtree.is_empty_tree()
                    });

                if only_delete_tree_if_empty && !is_empty {
                    return Ok(None);
                } else {
                    // TODO: dumb traversal should not be tolerated
                    for subtree_path in subtrees_paths {
                        merk_optional_tx!(
                            self.db,
                            subtree_path.iter().map(|x| x.as_slice()),
                            transaction,
                            mut subtree,
                            {
                                subtree.clear().map_err(|e| {
                                    Error::CorruptedData(format!(
                                        "unable to cleanup tree from storage: {}",
                                        e
                                    ))
                                })?;
                            }
                        );
                    }
                    delete_element()?;
                }
            } else {
                delete_element()?;
            }
            self.propagate_changes(path_iter, transaction)?;
            Ok(Some(element.node_byte_size(key.len())))
        }
    }

    // TODO: dumb traversal should not be tolerated
    /// Finds keys which are trees for a given subtree recursively.
    /// One element means a key of a `merk`, n > 1 elements mean relative path
    /// for a deeply nested subtree.
    pub(crate) fn find_subtrees<'p, P>(
        &self,
        path: P,
        transaction: TransactionArg,
    ) -> Result<Vec<Vec<Vec<u8>>>, Error>
    where
        P: IntoIterator<Item = &'p [u8]>,
    {
        // TODO: remove conversion to vec;
        // However, it's not easy for a reason:
        // new keys to enqueue are taken from raw iterator which returns Vec<u8>;
        // changing that to slice is hard as cursor should be moved for next iteration
        // which requires exclusive (&mut) reference, also there is no guarantee that
        // slice which points into storage internals will remain valid if raw iterator
        // got altered so why that reference should be exclusive;

        let mut queue: Vec<Vec<Vec<u8>>> =
            vec![path.into_iter().map(|x| x.as_ref().to_vec()).collect()];
        let mut result: Vec<Vec<Vec<u8>>> = queue.clone();

        while let Some(q) = queue.pop() {
            // Get the correct subtree with q_ref as path
            let path_iter = q.iter().map(|x| x.as_slice());
            storage_context_optional_tx!(self.db, path_iter.clone(), transaction, storage, {
                let mut raw_iter = Element::iterator(storage.raw_iter());
                while let Some((key, value)) = raw_iter.next()? {
                    if let Element::Tree(_) = value {
                        let mut sub_path = q.clone();
                        sub_path.push(key.to_vec());
                        queue.push(sub_path.clone());
                        result.push(sub_path);
                    }
                }
            })
        }
        Ok(result)
    }
}
