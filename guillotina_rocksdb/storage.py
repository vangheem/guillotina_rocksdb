import logging
import pickle

import rocksdb
from guillotina import configure
from guillotina.db import ROOT_ID
from guillotina.db.storages.base import BaseStorage
from guillotina.factory.content import Database
from guillotina.interfaces import IDatabaseConfigurationFactory
from guillotina_rocksdb.interfaces import IRocksdbStorage
from zope.interface import implementer

logger = logging.getLogger('guillotina')


@configure.utility(provides=IDatabaseConfigurationFactory, name="rocksdb")
async def CouchbaseDatabaseConfigurationFactory(key, dbconfig, loop=None):
    dss = RocksdbStorage(**dbconfig)
    if loop is not None:
        await dss.initialize(loop=loop)
    else:
        await dss.initialize()

    db = Database(key, dss)
    await db.initialize()
    return db


class AssocCounter(rocksdb.interfaces.AssociativeMergeOperator):
    def merge(self, key, existing_value, value):
        if key.startswith(b'__counter_'):
            if existing_value:
                s = int(existing_value) + int(value)
                return (True, str(s).encode('ascii'))
        else:
            # otherwise, we are merging a pickle with list/dict values
            if existing_value:
                existing_value = pickle.loads(existing_value)
                value = pickle.loads(value)
                if isinstance(existing_value, list):
                    existing_value.extend(value)
                else:
                    existing_value.update(value)
                return (True, pickle.dumps(existing_value))
        return (True, value)

    def name(self):
        return b'AssocCounter'


@implementer(IRocksdbStorage)
class RocksdbStorage(BaseStorage):
    """
    """

    _tid_counter_id = b'__counter_id'

    def __init__(self, read_only=False, filepath=None,
                 max_open_files=300000, write_buffer_size=67108864,
                 max_write_buffer_number=3,
                 target_file_size_base=67108864, **kwargs):
        opts = rocksdb.Options()
        opts.create_if_missing = True
        opts.max_open_files = max_open_files
        opts.write_buffer_size = write_buffer_size
        opts.max_write_buffer_number = max_write_buffer_number
        opts.target_file_size_base = target_file_size_base

        opts.table_factory = rocksdb.BlockBasedTableFactory(
            filter_policy=rocksdb.BloomFilterPolicy(10),
            block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
            block_cache_compressed=rocksdb.LRUCache(500 * (1024 ** 2)))
        opts.merge_operator = AssocCounter()

        self._db = rocksdb.DB(filepath, opts)
        super().__init__(read_only)

    async def finalize(self):
        pass

    async def initialize(self, loop=None):
        pass

    async def remove(self):
        """Reset the tables"""
        pass

    async def open(self):
        return self

    async def close(self, con):
        pass

    async def root(self):
        return await self.load(None, ROOT_ID)

    async def last_transaction(self, txn):
        return self._last_transaction

    async def get_next_tid(self, txn):
        if txn._tid is None:
            self._db.merge(self._tid_counter_id, b'1')
            txn._tid = int(self._db.get(self._tid_counter_id))
        return txn._tid

    async def load(self, txn, oid):
        value = self._db.get(b'object-' + oid.encode('utf-8'))
        if value is None:
            raise KeyError(oid)
        return pickle.loads(value)

    async def start_transaction(self, txn):
        pass

    def get_txn(self, txn):
        if not getattr(txn, '_db_txn', None):
            txn._db_txn = self
        return txn._db_txn

    async def store(self, oid, old_serial, writer, obj, txn):
        p = writer.serialize()  # This calls __getstate__ of obj
        part = writer.part
        if part is None:
            part = 0

        ob_id = b"object-" + oid.encode('utf-8')
        self._db.put(ob_id, pickle.dumps({
            'zoid': oid,
            'tid': txn._tid,
            'id': writer.id,
            'state': p
        }))

        if writer.of:
            self._db.merge(
                b'of-' + writer.of.encode('utf-8'), pickle.dumps({
                    writer.id: oid
                }))
        elif writer.parent_id:
            self._db.merge(
                b'keys-' + writer.parent_id.encode('utf-8'), pickle.dumps({
                    writer.id: oid
                }))

        return 0, len(p)

    async def delete(self, txn, oid):
        self._db.delete(b"object-" + oid.encode('utf-8'))

    async def commit(self, transaction):
        return await self.get_next_tid(transaction)

    async def abort(self, transaction):
        transaction._db_txn = None

    async def keys(self, txn, oid):
        keys = []
        value = self._db.get(b'keys-' + oid.encode('utf-8'))
        if value:
            return [{'id': i} for i in pickle.loads(value).keys()]
        return keys

    async def get_child(self, txn, parent_id, id):
        value = self._db.get(b'keys-' + parent_id.encode('utf-8'))
        if value:
            keys = pickle.loads(value)
            oid = keys[id]
            return await self.load(txn, oid)

    async def has_key(self, txn, parent_id, id):
        value = self._db.get(b'keys-' + parent_id.encode('utf-8'))
        if value:
            keys = pickle.loads(value)
            return id in keys
        return False

    async def len(self, txn, oid):
        value = self._db.get(b'keys-' + oid.encode('utf-8'))
        if value:
            keys = pickle.loads(value)
            return len(keys)
        return 0

    async def items(self, txn, oid):
        raise NotImplementedError()

    async def get_children(self, txn, parent, keys):
        items = []
        for key in keys:
            items.append(await self.get_child(txn, parent, key))
        return items

    async def get_annotation(self, txn, oid, id):
        value = self._db.get(b'of-' + oid.encode('utf-8'))
        if value:
            keys = pickle.loads(value)
            oid = keys[id]
            return await self.load(txn, oid)

    async def get_annotation_keys(self, txn, oid):
        keys = []
        value = self._db.get(b'of-' + oid.encode('utf-8'))
        if value:
            return [{'id': i} for i in pickle.loads(value).keys()]
        return keys

    async def del_blob(self, txn, bid):
        raise NotImplementedError()

    async def write_blob_chunk(self, txn, bid, oid, chunk_index, data):
        raise NotImplementedError()

    async def read_blob_chunk(self, txn, bid, chunk=0):
        raise NotImplementedError()

    async def get_conflicts(self, txn):
        return []

    async def get_page_of_keys(self, txn, oid, page=1, page_size=1000):
        raise NotImplementedError()
