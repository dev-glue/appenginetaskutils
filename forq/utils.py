from __future__ import absolute_import

import collections
import functools
import threading

from datetime import datetime
import hashlib
import time
from itertools import islice, chain

from cloudpickle import cloudpickle


def to_unix_timestamp(value, microseconds=None):
    ts = 0
    if isinstance(value, datetime):
        ts = time.mktime(value.timetuple())
        if microseconds:
            ts += (value.microsecond / 1e3)
    return ts


def utc_unix_timestamp(microseconds=None):
    return to_unix_timestamp(datetime.utcnow(), microseconds=microseconds)


def hash_function(func, *args, **kwargs):
    h = hash_encoded_function(encode_function(func, *args, **kwargs))
    return h


def hash_encoded_function(encoded_func):
    h = hashlib.md5(encoded_func).hexdigest()
    return h


def encode_function(func, *args, **kwargs):
    return cloudpickle.dumps((func, args, kwargs,))


def encode(*args, **kwargs):
    return cloudpickle.dumps((args, kwargs,))


def decode_function(pickled_data):
    # noinspection PyBroadException
    try:
        func, args, kwargs = cloudpickle.loads(pickled_data)
    except Exception:
        func, args, kwargs = None, None, None
    return func, args, kwargs


def decode(pickled_data):
    # noinspection PyBroadException
    try:
        # Should return a tuple (args, kwargs)
        return cloudpickle.loads(pickled_data)
    except Exception:
        pass


def hash_id(id_value):
    return hashlib.md5(id_value).hexdigest()


_NO_DEFAULT = object()  # so that None could be used as a default value also


def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


# noinspection SpellCheckingInspection
def iterchunks(iterable, size):
    """ Yield size chunks from iterable
    """
    source_iter = iter(iterable)
    while True:
        batch_iter = islice(source_iter, size)
        yield chain([batch_iter.next()], batch_iter)


# noinspection SpellCheckingInspection
def popattr(obj, name, default=_NO_DEFAULT):
    try:
        return obj.__dict__.pop(name)
    except KeyError:
        if default is not _NO_DEFAULT:
            return default
        raise AttributeError("no attribute '%s'" % name)
        # or getattr(obj, name) to generate a real AttributeError


def choose_int(*int_args):
    result = None
    for v in int_args:
        # noinspection PyBroadException
        try:
            result = int(v)
        except Exception:
            pass
    return result if result is not None else 0


class Struct(object):
    def __init__(self, **entries):
        if entries and len(entries):
            self.__dict__.update(entries)

    def __getattr__(self, name):
        if not name.startswith('_'):
            return self.__dict__.get(name)
        else:
            return super(Struct, self).__getattr__(name)

    def __setattr__(self, name, value):
        self.__dict__[name] = value

    def _to_dict(self):
        return self.__dict__.copy()

    def _populate(self, *args, **entries):
        self.__dict__.update(entries)
        for a in args:
            self.__dict__.update(a)

    def __repr__(self):
        return "Struct(" + unicode(self.__dict__) + ")"

    to_dict = _to_dict


class Hook(object):

    def __init__(self, *args):
        self._handlers = []
        for handler in flatten(args):
            self.add(handler)

    def add(self, handler):
        if callable(handler):
            self._handlers.append(handler)

    def __iadd__(self, handler):
        self.add(handler)
        return self

    def remove(self, handler):
        self._handlers.remove(handler)

    def __isub__(self, handler):
        self.remove(handler)
        return self

    def __call__(self, *args, **kwargs):
        for handler in self._handlers:
            # Is the callback an encoded function

            # noinspection PyBroadException
            try:
                handler(*args, **kwargs)
            except Exception:
                pass

    def __bool__(self):
        return len(self._handlers) > 0

    def __len__(self):
        return len(self._handlers)


def flatten(iterable, list_types=collections.Iterable):
    if not iterable:
        return
    remainder = iter(iterable)
    while True:
        first = next(remainder)
        if isinstance(first, list_types) and not isinstance(first, basestring):
            remainder = chain(first, remainder)
        else:
            yield first


class LockedCachedProperty(object):
    """
    A decorator for a locked lazy property like flask and webapp2 and Werkzueg
    """
    _not_run = object()

    def __init__(self, func, name=None, doc=None):
        self.__name__ = name or func.__name__
        self.__module__ = func.__module__
        self.__doc__ = doc or func.__doc__
        self.func = func
        self.lock = threading.RLock()

    def __get__(self, obj, lock_type=None):
        if obj is None:
            return self

        with self.lock:
            value = obj.__dict__.get(self.__name__, self._not_run)
            if value is self._not_run:
                value = self.func(obj)
                obj.__dict__[self.__name__] = value

            return value


class Sequencer(object):

    def __init__(self, items, start=None):
        self._cursor = start or 0
        self.items = items
        self.current = None
        self.last = None
        self.value = None

    def __iter__(self):
        return self

    def __len__(self):
        # noinspection PyBroadException
        try:
            count = len(self.items)
        except Exception:
            count = 0
        return count

    def has_size(self):
        # noinspection PyBroadException
        try:
            _ = len(self.items)
            r = True
        except Exception:
            r = False
        return r

    def next(self):

        try:
            self.current = self._cursor
            self._cursor += 1
            self.value = self.items[self.current]
        except IndexError:
            self.value = None
            self.current = None
            self._cursor = None
            raise StopIteration

        return self.current, self.value, self._cursor

    def cursor(self):
        return self._cursor
