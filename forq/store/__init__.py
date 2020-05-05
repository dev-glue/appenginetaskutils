from forq.future.exceptions import FutureStoreSaveFailed, FutureStoreCounterFailed
from forq.utils import decode_function

DEFAULT_STORE = None


def default_store_factory():
    if DEFAULT_STORE:
        return DEFAULT_STORE
    else:
        # noinspection PyBroadException
        try:
            from forq.contrib.appengine.store import AppEngineStore as DefaultStore
        except Exception:
            try:
                from forq.store.dictionary import DictStore as DefaultStore
            except ImportError:
                raise Exception("No store factory is available for use")

        return DefaultStore


class Counter(object):

    def __init__(self, key=None):
        self.key = key

    def reset(self):
        raise FutureStoreCounterFailed("Not implemented")

    def get(self):
        raise FutureStoreCounterFailed("Not implemented")

    def inc(self, amount=1):
        raise FutureStoreCounterFailed("Not implemented")

    def dec(self, amount=1):
        raise FutureStoreCounterFailed("Not implemented")


class Store(object):

    def __init__(self, *args, **kwargs):
        self.key = kwargs.pop('key', None)

    def _key(self, part=None):
        raise FutureStoreSaveFailed("Not implemented")

    def get(self, part=None):
        raise FutureStoreSaveFailed("Not implemented")

    def get_many(self, parts=None):
        raise FutureStoreSaveFailed("Not implemented")

    def exists(self, part=None):
        raise FutureStoreSaveFailed("Not implemented")

    def set(self, part=None, value=None):
        raise FutureStoreSaveFailed("Not implemented")

    def delete(self, part=None):
        raise FutureStoreSaveFailed("Not implemented")

    def clear(self):
        raise FutureStoreSaveFailed("Not implemented")

    def all(self):
        raise FutureStoreSaveFailed("Not implemented")

    def counter(self, part):
        raise FutureStoreSaveFailed("Not implemented")

    def to_state(self):
        raise FutureStoreSaveFailed("Not implemented")

    @classmethod
    def from_state(cls, state):
        # noinspection PyBroadException
        func, args, kwargs = decode_function(state)
        if func:
            # noinspection PyUnboundLocalVariable
            return func(*args, **kwargs)



