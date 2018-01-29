import functools
from taskutils.utils import hash_function
from google.appengine.api import memcache


# decorator to add caching to a function
def memcacher(f=None, cachekey=None, expiresec=600):
    if not f:
        return functools.partial(memcacher, cachekey=cachekey, expiresec=expiresec)

    def getvalue(*args, **kwargs):
        lcachekey = cachekey if cachekey else hash_function(f, *args, **kwargs)

        retval = memcache.get(lcachekey)  # @UndefinedVariable
        if retval is None:
            logdebug("MISS: %s" % lcachekey)
            retval = f(*args, **kwargs)
            memcache.add(key=lcachekey, value=retval, time=expiresec)  # @UndefinedVariable
        else:
            logdebug("HIT: %s" % lcachekey)

        return retval

    return getvalue
