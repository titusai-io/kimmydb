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

    /// Add a row to the served version vector that does not decode, so every
    /// read of the vector fails.
    pub fn corrupt_version_vector_for_test(&self) {
        let txn = self.begin_write(WriterHolder::Ddl).expect("the writer");
        {
            let mut versions = txn.open_table(tables::OPLOG_VERSIONS).expect("the table");
            versions
                .insert(b"not a node".as_slice(), b"not a clock".as_slice())
                .expect("the insert");
        }
        txn.commit().expect("the commit");
    }

    /// Overwrite every oplog entry with bytes that do not decode, so a read of
    /// the oplog that reaches one fails.
    pub fn corrupt_oplog_for_test(&self) {
        let txn = self.begin_write(WriterHolder::Ddl).expect("the writer");
        {
            let mut oplog = txn.open_table(tables::OPLOG).expect("the table");
            let keys: Vec<Vec<u8>> = redb::ReadableTable::iter(&oplog)
                .expect("the oplog")
                .map(|row| row.expect("a row").0.value().to_vec())
                .collect();
            for key in keys {
                oplog.insert(key.as_slice(), b"not an entry".as_slice()).expect("the insert");
            }
        }
        txn.commit().expect("the commit");
    }

    /// Overwrite one document's record with bytes that do not decode, so a
    /// write that re-reads it fails.
    pub fn corrupt_document_for_test(&self, coll: &CollectionMeta, id: &kimmy_core::DocId) {
        let key = crate::docs::doc_key(id).expect("an encodable id");
        self.store_undecodable_doc_for_test(coll, &key);
    }
}
