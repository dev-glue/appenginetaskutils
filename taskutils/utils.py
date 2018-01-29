from datetime import datetime
import hashlib
import time

import cloudpickle


def to_unix_timestamp(value, microseconds=None):
    ts = 0
    if isinstance(value, datetime):
        ts = time.mktime(value.timetuple())
        if microseconds:
            ts += (value.microsecond/1e3)
    return ts


def utc_unix_timestamp(microseconds=None):
    return to_unix_timestamp(datetime.utcnow(), microseconds=microseconds)


def hash_function(func, *args, **kwargs):
    h = hashlib.md5(encode_function(func, *args, **kwargs)).hexdigest()
    return h


def encode_function(func, *args, **kwargs):
    return cloudpickle.dumps((func, args, kwargs, ))


def decode_function(pickled_data):
    # Should return a tuple (func, args, kwargs)... do we need to check?
    return cloudpickle.loads(pickled_data)

