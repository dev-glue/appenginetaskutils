from __future__ import absolute_import

from random import randint

from google.appengine.ext import ndb
from google.appengine.ext.ndb import BlobProperty, StringProperty, IntegerProperty

from forq.store import Store, Counter
from forq.utils import decode, encode, encode_function


class StoreStorage(ndb.Model):
    prefix = StringProperty()
    prop = StringProperty(indexed=False)
    data = BlobProperty()


def make_appengine_store(key=None, namespace=None):
    return AppEngineStore(key=key, namespace=namespace)


COUNTER_SHARDS = 10


# Shard counter
class AppEngineCounter(Counter):

    def __init__(self, key=None, prefix=None, namespace=None):
        self.prefix = prefix
        self.namespace = namespace
        super(AppEngineCounter, self).__init__(key=key)

    def _key(self, shard):
        return ndb.Key(kind=StoreStorage, id="%s_%s_%s" % (self.prefix, self.key, shard), namespace=self.namesapce)

    def _keys(self):
        return [self._key(i) for i in xrange(COUNTER_SHARDS)]

    def reset(self):
        pass

    def _store(self, key, amount):
        StoreStorage(key=key, prefix=self.prefix, data=amount, namespace=self.namespace).put()

    def get(self):
        # noinspection PyBroadException
        try:
            total = 0
            for s in ndb.get_multi(self._keys()):
                if isinstance(s, StoreStorage):
                    # noinspection PyBroadException
                    try:
                        value = int(s.data)
                    except Exception:
                        value = 0
                    total += value
            for counter in StoreStorage.query():
                total += counter.count
        except Exception:
            total = 0
        return total

    def inc(self, amount=1):
        # Todo make this a shard counter
        @ndb.transactional(retries=5)
        def _inc():
            key = self._key(randint(1, COUNTER_SHARDS))
            # noinspection PyBroadException
            try:
                storage = key.get()
            except Exception:
                storage = None
            value = amount
            if storage:
                # noinspection PyBroadException
                try:
                    value += int(storage.data)
                except Exception:
                    pass
            self._store(key, value)

        return _inc()

    def dec(self, amount=1):
        self.inc(amount=-amount)


class AppEngineStore(Store):

    def __init__(self, *args, **kwargs):
        # Grab adapter namespace if it has one
        self.namespace = kwargs.pop('namespace', None)
        super(AppEngineStore, self).__init__(*args, **kwargs)

    def to_state(self):
        return encode_function(make_appengine_store, key=self.key, namespace=self.namespace)

    def _key(self, part=None):
        return ndb.Key(kind=StoreStorage, id="%s_%s" % (part, self.key or ""), namespace=self.namespace)

    def get(self, part=None):
        # noinspection PyBroadException
        try:
            key = self._key(part)
            storage = key.get()
        except Exception:
            storage = None

        return decode(storage.data) if storage else None

    def get_many(self, parts=None):
        keys = [self._key(k) for k in parts]
        # noinspection PyBroadException
        try:
            result = [decode(storage.data) for storage in ndb.get_multi(keys) if storage and storage.data]
        except Exception:
            result = []
        return result

    def set(self, part=None, value=None, counter=False):
        key = self._key(part)
        StoreStorage(key=key, prefix=self.key, data=encode(value)).put()

    def delete(self, key=None):
        storage_key = self._key(key)
        storage_key.delete()

    def _query(self):
        return StoreStorage.query(StoreStorage.prefix == self.key, namespace=self.namespace)

    def clear(self):
        ndb.delete_multi([k for k in self._query().iter(keys_only=True) if k])

    def all(self):
        return {s.prop: decode(s.data) for s in self._query().iter() if s}

    def exists(self, part=None):
        # noinspection PyBroadException
        try:
            storage_key = self._key(part)
            storage = storage_key.get()
        except Exception:
            storage = None
        return bool(storage)

    def counter(self, part):
        return AppEngineCounter(part, prefix=self.key, namespace=self.namespace)
