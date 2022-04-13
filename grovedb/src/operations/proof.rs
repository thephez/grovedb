use std::io::{Read, Write};

use merk::proofs::query::QueryItem;
use rs_merkle::{algorithms::Sha256, MerkleProof};
use storage::rocksdb_storage::RocksDbStorage;

use crate::{
    merk::ProofConstructionResult,
    subtree::raw_decode,
    util::{merk_optional_tx, meta_storage_context_optional_tx},
    Element, Error,
    Error::{InvalidPath, InvalidProof},
    GroveDb, PathQuery, Query, SizedQuery,
};

const EMPTY_TREE_HASH: [u8; 32] = [0; 32];

#[derive(Debug)]
enum ProofType {
    MerkProof,
    SizedMerkProof,
    RootProof,
    InvalidProof,
}

impl From<ProofType> for u8 {
    fn from(proof_type: ProofType) -> Self {
        match proof_type {
            ProofType::MerkProof => 0x01,
            ProofType::SizedMerkProof => 0x02,
            ProofType::RootProof => 0x03,
            ProofType::InvalidProof => 0x10,
        }
    }
}

impl From<u8> for ProofType {
    fn from(val: u8) -> Self {
        match val {
            0x01 => ProofType::MerkProof,
            0x02 => ProofType::SizedMerkProof,
            0x03 => ProofType::RootProof,
            _ => ProofType::InvalidProof,
        }
    }
}

fn write_to_vec<W: Write>(dest: &mut W, value: &Vec<u8>) {
    dest.write_all(value);
}

// TODO: Delete this function
fn print_path(path: Vec<&[u8]>) {
    let mut result = String::from("");
    for p in path {
        let m = std::str::from_utf8(p).unwrap();
        result.push_str(" -> ");
        result.push_str(m);
    }
    dbg!(result);
}

impl GroveDb {
    pub fn prove(&self, query: PathQuery) -> Result<Vec<u8>, Error> {
        // TODO: Should people be allowed to get proofs for tree items?? defaulting to
        // yes
        let mut proof_result: Vec<u8> = vec![];

        let path_slices = query.path.iter().map(|x| x.as_slice()).collect::<Vec<_>>();

        self.check_subtree_exists_path_not_found(path_slices.clone(), None, None)?;

        let mut current_limit: Option<u16> = query.query.limit;
        let mut current_offset: Option<u16> = query.query.offset;

        // TODO: Figure out references

        // There is nothing stopping a tree from having a combination of different
        // element types TODO: Figure out how to properly deal with references
        // There is a possibility that the result set might span multiple trees if the
        // subtrees hold different element types (brings too much complexity,
        // will ignore for now) TODO: What to do if a subtree returns a
        // combination of item, reference and tree elements TODO: How would they
        // be added to the result set, limit, offset e.t.c.

        prove_subqueries(
            &self,
            &mut proof_result,
            path_slices.clone(),
            query.clone(),
            &mut current_limit,
            &mut current_offset,
        )
        .unwrap();

        fn prove_subqueries(
            db: &GroveDb,
            proofs: &mut Vec<u8>,
            path: Vec<&[u8]>,
            query: PathQuery,
            current_limit: &mut Option<u16>,
            current_offset: &mut Option<u16>,
        ) -> Result<(), Error> {
            merk_optional_tx!(db.db, path.clone(), None, subtree, {
                // TODO: Not allowed to create proof for an empty tree (handle this)

                let mut has_useful_subtree = false;

                let exhausted_limit =
                    query.query.limit.is_some() && query.query.limit.unwrap() == 0;

                if !exhausted_limit {
                    let subtree_key_values = subtree.get_kv_pairs(query.query.query.left_to_right);
                    for (key, value_bytes) in subtree_key_values.iter() {
                        let (subquery_key, subquery_value) =
                            Element::subquery_paths_for_sized_query(&query.query, key);

                        if subquery_key.is_none() && subquery_value.is_none() {
                            continue;
                        }

                        // TODO: Figure out what to do if decoding fails
                        let element = raw_decode(value_bytes).unwrap();

                        match element {
                            Element::Tree(tree_hash) => {
                                // skip proof generation for empty trees
                                if tree_hash == EMPTY_TREE_HASH {
                                    continue;
                                }

                                // once we know a tree has a subtree + there is a subquery
                                // we can prove the current tree
                                if !has_useful_subtree {
                                    has_useful_subtree = true;

                                    let mut all_key_query =
                                        Query::new_with_direction(query.query.query.left_to_right);
                                    all_key_query.insert_all();

                                    // generate unsized merk proof for current element
                                    // TODO: Remove duplication
                                    // TODO: How do you handle mixed tree types?
                                    let ProofConstructionResult { proof, .. } = subtree
                                        .prove(all_key_query, None, None)
                                        .expect("should generate proof");

                                    // TODO: Switch to variable length encoding
                                    debug_assert!(proof.len() < 256);
                                    write_to_vec(
                                        proofs,
                                        &vec![ProofType::MerkProof.into(), proof.len() as u8],
                                    );
                                    write_to_vec(proofs, &proof);
                                }

                                // TODO: cleanup
                                let mut new_path = path.clone();
                                new_path.push(key.as_ref());

                                let mut query = subquery_value.clone();
                                let sub_key = subquery_key.clone();

                                if query.is_some() {
                                    if sub_key.is_some() {
                                        // intermediate step here, generate a proof that the
                                        // subquery key
                                        // exists or doesn't exist in this subtree
                                        merk_optional_tx!(
                                            db.db,
                                            new_path.clone(),
                                            None,
                                            inner_subtree,
                                            {
                                                // // generate a proof for the subquery key
                                                // dbg!(std::str::from_utf8(
                                                //     sub_key.clone().unwrap().as_slice()
                                                // ));
                                                // no need for direction as it's a single key query
                                                let mut key_as_query = Query::new();
                                                key_as_query.insert_key(sub_key.clone().unwrap());
                                                // query = Some(key_as_query);

                                                let ProofConstructionResult { proof, .. } =
                                                    inner_subtree
                                                        .prove(key_as_query.clone(), None, None)
                                                        .expect("should generate proof");
                                                dbg!(&proof);

                                                debug_assert!(proof.len() < 256);
                                                write_to_vec(
                                                    proofs,
                                                    &vec![
                                                        ProofType::MerkProof.into(),
                                                        proof.len() as u8,
                                                    ],
                                                );
                                                write_to_vec(proofs, &proof);
                                            }
                                        );

                                        new_path.push(sub_key.as_ref().unwrap());
                                        // verify that the new path exists
                                        let subquery_key_path_exists = db
                                            .check_subtree_exists_path_not_found(
                                                new_path.clone(),
                                                None,
                                                None,
                                            );
                                        if subquery_key_path_exists.is_err() {
                                            continue;
                                        }
                                    }
                                } else {
                                    // only subquery key must exist, convert to query
                                    // no need for direction as it's a single key
                                    let mut key_as_query = Query::new();
                                    key_as_query.insert_key(sub_key.unwrap());
                                    query = Some(key_as_query);
                                }

                                // dbg!(&new_path);
                                // dbg!(&query);

                                let new_path_owned = new_path.iter().map(|x| x.to_vec()).collect();
                                // TODO: Figure out why this is not sized
                                let new_path_query =
                                    PathQuery::new_unsized(new_path_owned, query.unwrap());

                                // add proofs for child nodes
                                // TODO: Handle error properly, what could cause an error?
                                prove_subqueries(
                                    db,
                                    proofs,
                                    new_path,
                                    new_path_query,
                                    current_limit,
                                    current_offset,
                                )
                                .unwrap();

                                // if we hit the limit, we should kill the loop
                                if current_limit.is_some() && current_limit.unwrap() == 0 {
                                    break;
                                }
                            }
                            _ => {
                                // Current implementation makes the assumption that all elements of
                                // a result set are of the same type i.e either all trees, all items
                                // e.t.c and not mixed types.
                                // This catches when that invariant is not preserved.
                                debug_assert!(has_useful_subtree == false);
                            }
                        }
                    }
                }

                if !has_useful_subtree {
                    // if no useful subtree, then we care about the result set of this subtree
                    // apply sized query
                    let proof_result = subtree
                        .prove(query.query.query, *current_limit, *current_offset)
                        .expect("should generate proof");

                    // update limit and offset values
                    *current_limit = proof_result.limit;
                    *current_offset = proof_result.offset;

                    // only adding to the proof result set, after you have added that of
                    // your child nodes
                    // TODO: Switch to variable length encoding
                    debug_assert!(proof_result.proof.len() < 256);
                    write_to_vec(
                        proofs,
                        &vec![
                            ProofType::SizedMerkProof.into(),
                            proof_result.proof.len() as u8,
                        ],
                    );
                    write_to_vec(proofs, &proof_result.proof);
                }
            });

            Ok(())
        }

        // generate proof up to root
        let mut split_path = path_slices.split_last();
        while let Some((key, path_slice)) = split_path {
            if path_slice.is_empty() {
                // generate root proof
                meta_storage_context_optional_tx!(self.db, None, meta_storage, {
                    let root_leaf_keys = Self::get_root_leaf_keys_internal(&meta_storage)?;
                    let mut root_index: Vec<usize> = vec![];
                    match root_leaf_keys.get(&key.to_vec()) {
                        Some(index) => root_index.push(*index),
                        None => return Err(InvalidPath("invalid root key")),
                    }
                    let root_tree = self.get_root_tree(None).expect("should get root tree");
                    let root_proof = root_tree.proof(&root_index).to_bytes();

                    debug_assert!(root_proof.len() < 256);
                    write_to_vec(
                        &mut proof_result,
                        &vec![ProofType::RootProof.into(), root_proof.len() as u8],
                    );
                    write_to_vec(&mut proof_result, &root_proof);

                    // write the number of root leafs
                    // makes the assumption that 1 byte is enough to represent the root leaf count
                    // size
                    write_to_vec(&mut proof_result, &vec![root_leaf_keys.len() as u8]);

                    // add the index values required to prove the root
                    let root_index_bytes = root_index
                        .into_iter()
                        .map(|index| index as u8)
                        .collect::<Vec<u8>>();

                    write_to_vec(&mut proof_result, &root_index_bytes);
                })
            } else {
                let path_slices = path_slice.iter().map(|x| *x).collect::<Vec<_>>();

                merk_optional_tx!(self.db, path_slices, None, subtree, {
                    // TODO: Not allowed to create proof for an empty tree (handle this)
                    let mut query = Query::new();
                    query.insert_key(key.to_vec());

                    let ProofConstructionResult { proof, .. } = subtree
                        .prove(query, None, None)
                        .expect("should generate proof");

                    debug_assert!(proof.len() < 256);
                    write_to_vec(
                        &mut proof_result,
                        &vec![ProofType::MerkProof.into(), proof.len() as u8],
                    );
                    write_to_vec(&mut proof_result, &proof);
                });
            }
            split_path = path_slice.split_last();
        }

        Ok(proof_result)
    }

    // TODO: Audit and make clearer logic of figuring out what to verify
    // TODO: There might be cases where we randomly stop proof generations because a
    // TODO: certain key does not exists, should also take that into account
    pub fn execute_proof(
        proof: &[u8],
        query: PathQuery,
    ) -> Result<([u8; 32], Vec<(Vec<u8>, Vec<u8>)>), Error> {
        let path_slices = query.path.iter().map(|x| x.as_slice()).collect::<Vec<_>>();

        // global result set
        let mut result_set: Vec<(Vec<u8>, Vec<u8>)> = vec![];
        // initialize proof reader
        let mut proof_reader = ProofReader::new(proof);
        let mut current_limit = query.query.limit;
        let mut current_offset = query.query.offset;

        // not sure the type of the initial merk proof (might be sized or unsized, but
        // should be merk proof right??) TODO: Is there a possibility that there
        // might only be a root proof (handle accordingly) maybe use the length
        // of the path to determine this?? assuming it starts with a merk proof

        // TODO: optionally run this (check if the proof is only for root)
        // TODO: Get rid of clone
        let mut last_root_hash = execute_subquery_proof(
            &mut proof_reader,
            &mut result_set,
            &mut current_limit,
            &mut current_offset,
            query.clone(),
        )?;

        fn execute_subquery_proof(
            proof_reader: &mut ProofReader,
            result_set: &mut Vec<(Vec<u8>, Vec<u8>)>,
            current_limit: &mut Option<u16>,
            current_offset: &mut Option<u16>,
            query: PathQuery,
        ) -> Result<[u8; 32], Error> {
            let root_hash: [u8; 32];
            let (proof_type, proof) = proof_reader.read_proof()?;
            match proof_type {
                ProofType::SizedMerkProof => {
                    // TODO: remove expect clause + clone
                    let verification_result = merk::execute_proof(
                        &proof,
                        &query.query.query,
                        current_limit.clone(),
                        current_offset.clone(),
                        query.query.query.left_to_right,
                    )
                    .expect("should execute proof");

                    root_hash = verification_result.0;
                    result_set.extend(verification_result.1.result_set);

                    // update limit and offset
                    *current_limit = verification_result.1.limit;
                    *current_offset = verification_result.1.offset;
                }
                ProofType::MerkProof => {
                    // TODO: remove expect clause

                    // for non leaf subtrees, we want to prove that all their keys
                    // have an accompanying proof as long as the limit is non zero
                    // and their child subtree is not empty
                    let mut all_key_query =
                        Query::new_with_direction(query.query.query.left_to_right);
                    all_key_query.insert_all();

                    let verification_result = merk::execute_proof(
                        &proof,
                        &all_key_query,
                        None,
                        None,
                        all_key_query.left_to_right,
                    )
                    .expect("should execute proof");

                    root_hash = verification_result.0;

                    // TODO: remove clone
                    for (key, value_bytes) in verification_result.1.result_set.clone() {
                        // we use the key to get the exact subquery
                        // TODO: Handle limits
                        // recurse with the new subquery
                        // verify that the root hash is what you expect
                        // value must represent a tree (error if it does not)
                        // maybe err first
                        // TODO: Remove duplication
                        let child_element = Element::deserialize(value_bytes.as_slice())?;
                        match child_element {
                            Element::Tree(mut expected_root_hash) => {
                                if expected_root_hash == EMPTY_TREE_HASH {
                                    // child node is empty, move on to next
                                    continue;
                                }

                                if current_limit.is_some() && current_limit.unwrap() == 0 {
                                    // we are done verifying the subqueries
                                    break;
                                }

                                let (subquery_key, subquery_value) =
                                    Element::subquery_paths_for_sized_query(
                                        &query.query,
                                        key.as_slice(),
                                    );

                                if subquery_value.is_none() && subquery_key.is_none() {
                                    continue;
                                }

                                // what do you do if there exists a subquery key
                                // if there is a subquery key then there would be a corresponding
                                // proof to prove it's existence.
                                // if it does not exist in the result set then stop
                                // if it does exists in the result set, update the expected root
                                // hash and continue
                                if subquery_key.is_some() {
                                    let (proof_type, subkey_proof) = proof_reader.read_proof()?;
                                    // TODO: verify it's a merk proof

                                    let mut key_as_query = Query::new();
                                    key_as_query.insert_key(subquery_key.clone().unwrap());

                                    let verification_result = merk::execute_proof(
                                        &subkey_proof,
                                        &key_as_query,
                                        None,
                                        None,
                                        key_as_query.left_to_right,
                                    );

                                    let rset = verification_result.unwrap().1.result_set;
                                    if rset.len() == 0 {
                                        // subquery key does not exist in the subtree
                                        // proceed to another subtree
                                        continue;
                                    } else {
                                        let elem_value = &rset[0].1;
                                        let elem = Element::deserialize(elem_value).unwrap();
                                        match elem {
                                            Element::Tree(new_exptected_hash) => {
                                                expected_root_hash = new_exptected_hash;
                                            }
                                            _ => {
                                                // TODO: Remove panic
                                                panic!("figure out what to do in this case");
                                            }
                                        }
                                    }
                                }

                                let new_path_query;
                                if subquery_value.is_some() {
                                    new_path_query =
                                        PathQuery::new_unsized(vec![], subquery_value.unwrap());
                                } else {
                                    let mut key_as_query = Query::new();
                                    key_as_query.insert_key(subquery_key.unwrap());
                                    new_path_query = PathQuery::new_unsized(vec![], key_as_query);
                                }

                                let child_hash = execute_subquery_proof(
                                    proof_reader,
                                    result_set,
                                    current_limit,
                                    current_offset,
                                    new_path_query,
                                )?;

                                if child_hash != expected_root_hash {
                                    return Err(Error::InvalidProof(
                                        "child hash doesn't match the expected hash",
                                    ));
                                }
                            }
                            _ => {
                                // TODO: why this error??
                                return Err(Error::InvalidProof("Missing proof for subtree"));
                            }
                        }
                    }
                }
                _ => {
                    // TODO: Update here when you fix possibility of only root
                    return Err(Error::InvalidProof("wrong proof type"));
                }
            }
            Ok(root_hash)
        }

        // Validate the path
        let mut split_path = path_slices.split_last();
        while let Some((key, path_slice)) = split_path {
            if !path_slice.is_empty() {
                let merk_proof = proof_reader.read_proof_of_type(ProofType::MerkProof.into())?;

                let mut parent_query = Query::new();
                parent_query.insert_key(key.to_vec());

                // TODO: Handle this better, should not be using expect
                let proof_result = merk::execute_proof(
                    &merk_proof,
                    &parent_query,
                    None,
                    None,
                    query.query.query.left_to_right,
                )
                .expect("should execute proof");
                let result_set = proof_result.1.result_set;

                if result_set[0].0 != key.to_vec() {
                    return Err(Error::InvalidProof("proof invalid: invalid parent"));
                }
                let elem = Element::deserialize(result_set[0].1.as_slice())?;
                let child_hash = match elem {
                    Element::Tree(hash) => Ok(hash),
                    _ => Err(Error::InvalidProof(
                        "intermediate proofs should be for trees",
                    )),
                }?;

                if child_hash != last_root_hash {
                    return Err(Error::InvalidProof("Bad path"));
                }

                last_root_hash = proof_result.0;
            } else {
                break;
            }
            split_path = path_slice.split_last();
        }

        let root_proof = proof_reader.read_proof_of_type(ProofType::RootProof.into())?;

        // makes the assumption that 1 byte is enough to represent the root leaf count
        // size
        let root_leaf_size = proof_reader.read_byte()?;

        let root_meta_data = proof_reader.read_to_end();
        let root_index_usize = root_meta_data
            .into_iter()
            .map(|index| index as usize)
            .collect::<Vec<usize>>();

        // TODO: Rename
        let root_proof_terrible_name = match MerkleProof::<Sha256>::try_from(root_proof) {
            Ok(proof) => Ok(proof),
            Err(_) => Err(Error::InvalidProof("invalid proof element")),
        }?;

        // getting rid of the leaf count:
        // for our purposes, root leafs are not expected to be very many
        // could theoretically be represented with just one byte
        // but nothing about the system prevents more than one byte of leaf keys
        let root_hash = match root_proof_terrible_name.root(
            &root_index_usize,
            &[last_root_hash],
            root_leaf_size[0] as usize,
        ) {
            Ok(hash) => Ok(hash),
            Err(_) => Err(Error::InvalidProof("Invalid proof element")),
        }?;

        Ok((root_hash, result_set))
    }
}

#[derive(Debug)]
struct ProofReader<'a> {
    proof_data: &'a [u8],
}

impl<'a> ProofReader<'a> {
    fn new(proof_data: &'a [u8]) -> Self {
        Self { proof_data }
    }

    fn read_byte(&mut self) -> Result<[u8; 1], Error> {
        let mut data = [0; 1];
        self.proof_data.read(&mut data);
        Ok(data)
    }

    // TODO: Handle duplication
    // TODO: handle error (e.g. not enough bytes to read)
    fn read_proof(&mut self) -> Result<(ProofType, Vec<u8>), Error> {
        let mut data_type = [0; 1];
        self.proof_data.read(&mut data_type);

        let proof_type: ProofType = data_type[0].into();

        let mut length = vec![0; 1];
        self.proof_data.read(&mut length);
        let mut proof = vec![0; length[0] as usize];
        self.proof_data.read(&mut proof);

        Ok((proof_type, proof))
    }

    fn read_proof_of_type(&mut self, expected_data_type: u8) -> Result<Vec<u8>, Error> {
        let mut data_type = [0; 1];
        self.proof_data.read(&mut data_type);

        if data_type != [expected_data_type] {
            return Err(Error::InvalidProof("wrong data_type"));
        }

        let mut length = vec![0; 1];
        self.proof_data.read(&mut length);
        let mut proof = vec![0; length[0] as usize];
        self.proof_data.read(&mut proof);

        Ok(proof)
    }

    fn read_to_end(&mut self) -> Vec<u8> {
        let mut data = vec![];
        self.proof_data.read_to_end(&mut data);
        data
    }
}
