use crate::{GroveDB};

pub struct Transaction<'a> {
    groveDB: &'a GroveDB
}

impl Transaction {
    pub fn new(merk: &GroveDB) -> Self {
        Transaction { groveDB: merk }
    }

    pub fn insert(&mut self, batch: &Batch, aux: &Batch) -> Result<()> {
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(Option(Vec::new()))
    }

    pub fn get_raw(&self) {

    }

    /// Commits the data from the transaction
    pub fn commit(self) {}

    /// Cancels the transaction
    pub fn rollback(self) {}
}