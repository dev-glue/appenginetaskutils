from multiprocessing import Manager, Pipe, Process

from forq.store import Counter, Store
from forq.utils import encode_function, decode, encode


def make_dict_store(key=None, storage=None):
    return DictStore(key=key, storage=storage)


class DictCounter(Counter):

    def __init__(self, key=None, data=None):
        self.data = data
        super(DictCounter, self).__init__(key=key)

    def reset(self):
        value = self.data[self.key] = 0
        return value

    def get(self):
        # noinspection PyBroadException
        try:
            value = int(self.data.get(self.key, None))
        except Exception:
            value = 0
        return value

    def inc(self, amount=1):
        # noinspection PyBroadException
        try:
            value = self.data.get(self.key, None)
            value += amount
        except Exception:
            value = 1
        self.data[self.key] = value
        return value

    def dec(self, amount=1):
        return self.inc(amount=-amount)


class DictStore(Store):

    def __init__(self, *args, **kwargs):
        storage = kwargs.pop('storage', None)
        if storage is None:
            storage = dict()

        self.storage = storage
        super(DictStore, self).__init__(*args, **kwargs)

    def to_state(self):
        return encode_function(make_dict_store, key=self.key, storage=self.storage)

    def _key(self, part=None):
        return "%s_%s" % (self.key or "", part)

    def get(self, part=None):
        v = self.storage.get(self._key(part), None)
        if v is not None:
            (args, kwargs) = decode(v)
            return args[0] if args else None

    def get_many(self, parts=None):
        return [self.get(part) for part in parts]

    def exists(self, part=None):
        return self._key(part) in self.storage

    def set(self, part=None, value=None):
        key = self._key(part)
        v = encode(value)
        self.storage[key] = v
        v = None

    def delete(self, part=None):
        key = self._key(part)
        del self.storage[key]

    def clear(self):
        self.storage.clear()

    def all(self):
        return [self.get(k) for k in self.storage.iterkeys()]

    def counter(self, part):
        return DictCounter(self._key(part), self.storage)

