//! Records no write path can produce, for the tests of readers that must
//! survive them (ADR-187): a document record that does not decode, and a
//! collection whose metadata does not. Under `cfg(test)` and the `test-hooks`
//! feature, which only another crate's dev-dependency enables, so absent from
//! every build that ships.
//!
//! Exempt from `every_document_write_moves_the_count` by name: a record that
//! does not decode is not a document, and moving the live count for it would
//! be the drift that guard exists to stop.

use crate::engine::{Engine, WriterHolder};
use crate::meta::CollectionMeta;
use crate::tables;

impl Engine {
    /// Store bytes that do not decode as a document under `key` in `coll`.
    pub fn store_undecodable_doc_for_test(&self, coll: &CollectionMeta, key: &[u8]) {
        let txn = self.begin_write(WriterHolder::Write).expect("the writer");
        {
            let mut docs = txn.open_table(tables::DOCS).expect("the documents table");
            docs.insert((coll.id.0, key), b"not a document record".as_slice()).expect("the insert");
        }
        txn.commit().expect("the commit");
    }

    /// Overwrite a collection's metadata with bytes that do not decode, so
    /// every read of the collection fails rather than finding it absent.
    pub fn corrupt_collection_meta_for_test(&self, db: &str, name: &str) {
        let txn = self.begin_write(WriterHolder::Ddl).expect("the writer");
        {
            let mut collections = txn.open_table(tables::COLLECTIONS).expect("the table");
            collections.insert((db, name), b"not metadata".as_slice()).expect("the insert");
        }
        txn.commit().expect("the commit");
    }
}
