
class FutureNotImplementedError(Exception):
    pass


class FutureReadyForResult(Exception):
    pass


class FutureNotReadyForResult(Exception):
    pass


class FutureTimedOutError(Exception):
    pass


class FutureCancelled(Exception):
    pass


class FutureStoreSaveFailed(Exception):
    pass


class FutureStoreLoadFailed(Exception):
    pass


class FutureStoreCreateFailed(Exception):
    pass


class FutureStoreKeyInvalid(Exception):
    pass


class FutureStoreKeyMissing(Exception):
    pass


class FutureStoreCounterFailed(Exception):
    pass


class ContinueFuture(Exception):
    def __init__(self, func, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.func = func

