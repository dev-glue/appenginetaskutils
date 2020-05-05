import datetime
import logging

from forq.exceptions import PermanentTaskFailure, SingularTaskFailure, IgnoredTaskFailure
from forq.future.exceptions import ContinueFuture
from forq.store import Store, default_store_factory
from forq.task import Task, TaskDecorator
from forq.utils import flatten, encode_function, decode_function

DEFAULT_FUTURE_STORE_FACTORY = None


# noinspection PyBroadException
def run_future(state, encoded_func, *args, **context):

    if isinstance(state, Future):
        f = state
    else:
        f = Future.from_state(state)
    if not isinstance(f, Future):
        # Assume future has been 'removed'
        # Silently fail
        return
    func, args, kwargs = decode_function(encoded_func)
    if func is None:
        # Assume if we can't decode a function then func is a 'result' of a completed Future
        func = encoded_func
        args = []
        kwargs = {}
    return f.run(func, args, kwargs, **context)


def make_future(state, factory=None, **extra_kwargs):

    store = Store.from_state(state)
    # Prep data
    kwargs = store.get('future')
    if kwargs:
        args = kwargs.pop("args", [])
        kwargs.update(kwargs.pop('kwargs', {}))
        kwargs.update(extra_kwargs)
        factory = factory or Future
        # Re-create future if available
        return factory(store=store, *args, **kwargs)


def expires(timeout):
    return datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout) if timeout else None


class Future(object):
    key = None
    callbacks = None
    queue = None
    store = None
    func = None
    args = None
    kwargs = None
    completed = False
    error = None
    started = False

    def __init__(self, *args, **kwargs):
        # Attach queue and store
        self.queue = kwargs.pop("queue", None)
        self.store = kwargs.pop("store", None)
        # Attach functions
        self.callbacks = kwargs.pop("callbacks", None)
        self.func = kwargs.pop("func", None)
        # Handle retry process
        self.max_retries = kwargs.pop("max_retries", 0)
        # Handle timeouts
        self.timeout = kwargs.pop("timeout", None)
        # Capture other arguments
        self.args = list(args)
        self.kwargs = kwargs
        self.func_args = None
        self.func_kwargs = None

    @classmethod
    def constructor(cls):
        return make_future

    def to_dict(self):
        d = self.__dict__.copy()
        d.pop('store', None)
        return d

    @classmethod
    def from_state(cls, state):
        # noinspection PyBroadException
        func, args, kwargs = decode_function(state)
        if func:
            # noinspection PyUnboundLocalVariable
            return func(*args, **kwargs)

    def save(self):
        d = self.to_dict()
        self.store.set('future', d)

    def clear(self):
        # Clear store (which means any other running queued item wont be able to make_future)
        self.store.clear()

    def always(self, *args, **kwargs):
        # Check if done... then clear store (which means any other running task wont be able to make_future)
        if self.completed:
            self._callback('always', *args, **kwargs)
            self.clear()

    def success(self, *args, **kwargs):
        if self.completed:
            # Process success
            self._callback('success', *args, **kwargs)

    def fail(self, *args, **kwargs):
        if self.error:
            # Process success
            self._callback('fail', *args, **kwargs)

    def to_state(self):
        return encode_function(self.constructor(), self.store.to_state())

    def _callback(self, key, *args, **kwargs):
        for handler in flatten(self.callbacks.get(key, None) or []):
            if callable(handler):
                # noinspection PyBroadException
                try:
                    handler(*args, **kwargs)
                except Exception as e:
                    logging.error("Future callback error")

    def _func(self, func, *args, **kwargs):
        return True, func(*args, **kwargs)

    def _retry_counter(self):
        # Check for retries and timeouts
        return self.store.counter('retries')

    # noinspection PyUnusedLocal
    def run(self, func, args, kwargs, **context):

        if self.completed:
            return  # Don't bother running

        context['future'] = self

        # Short cut a function trying to run an already completed function
        error = context.pop('error', None)
        context.update(**kwargs)

        # Get max retries
        max_retries = self.max_retries

        # Check for retries and timeouts
        retry_counter = self._retry_counter() if max_retries else None
        retries = retry_counter.get() if retry_counter else 0

        if self.timeout is not None and expires(self.timeout) < datetime.datetime.utcnow():
            error = PermanentTaskFailure("Future has timed out after %s seconds" % self.timeout)

        result = None
        if error is None:
            if callable(func):
                try:
                    completed, result = self._func(func, *args, **context)
                except ContinueFuture as cf:
                    func = cf.func if callable(cf.func) else func
                    self.enqueue(func, *cf.args, **cf.kwargs)
                    error = None
                    completed = False
                except Exception as err:
                    error = err
                    completed = False
                self.completed = completed
            else:
                # Assume a non-callable func indicates a result passed after a delay
                result = func
                self.completed = True

        if isinstance(error, SingularTaskFailure):
            if max_retries and retries >= max_retries:
                error = PermanentTaskFailure("Future function failed (Attempt %s of %s). Error: %s" % (retries, max_retries, error.message))
            else:
                # increment retry counter
                if retry_counter:
                    retry_counter.inc()
                raise error

        self.error = error
        #  Handler error
        if self.error is not None:

            # Handle failure if not already marked as Ignore
            if not isinstance(self.error, IgnoredTaskFailure):
                self.fail(*args, error=self.error, **context)

            # Manipulate if error if told to ignore it
            if isinstance(self.error, IgnoredTaskFailure):
                error = self.error = None
                result = None
            else:
                error = PermanentTaskFailure("Future failed. Error: %s" % error.message)
            # Force completed on error - future task shouldn't run again
            self.completed = True

        elif self.completed:
            # only process success on completed
            self.success(*args, result=result, **context)

        # Process callback
        self.always(*args, result=result, error=self.error, **context)

        if error:
            raise error
        else:
            return result

    def enqueue(self, func, *args, **kwargs):
        state = self.to_state()
        encoded_func = encode_function(func, *args, **kwargs)
        return self.queue.add(run_future, state, encoded_func)

    def start(self, func, *args, **kwargs):
        # Save the initial Future object to its store so it can be recovered (include initial func args and kwargs)
        self.save()

        # Enqueue the Future 'run function'
        task = self.enqueue(func, *args, **kwargs)
        if self.timeout:
            # Handle a timeout
            if self.queue and 'run_at' in self.queue.supports:
                self.enqueue(func, *args, _run_at=expires(self.timeout), **kwargs)
        return task


class FutureTask(Task):
    _factory = Future

    def __init__(self, *args, **kwargs):
        # Register callbacks in the kwargs

        self._callbacks = {}
        self._store = kwargs.pop("store", DEFAULT_FUTURE_STORE_FACTORY)

        for k in ['success', 'failure', 'always']:
            self._register_callback(k, kwargs.pop(k, getattr(self, '_%s' % k, None)))

        super(FutureTask, self).__init__(*args, **kwargs)

    def _register_callback(self, key, callback):
        if callback and callable(callback):
            self._callbacks.setdefault(key, []).append(callback)

    def success(self, func):
        self._register_callback("success", func)
        return self

    def fail(self, func):
        self._register_callback("fail", func)
        return self

    def always(self, func):
        self._register_callback("always", func)
        return self

    def _future(self, queue, store, *args, **kwargs):

        # Get queue
        f = self._factory(
            queue=queue,
            store=store,
            callbacks=self._callbacks,
            *args,
            **kwargs
        )
        return f

    @property
    def store(self):
        if not self._store:
            store_factory = default_store_factory()
            k = "future:%s" % self.key
            self._store = store_factory(key=k)
        return self._store

    def _task(self, *args, **kwargs):
        # Rather than just put function on the queue the FutureTask creates a Future and gets it to put itself on queue
        # Get queue
        q = self.queue
        # Get store
        s = self.store
        # Get timeout
        timeout = kwargs.get('timeout', None)
        # Create a future
        f = self._future(queue=q, store=s, timeout=timeout, *self._args, **self._kwargs)
        # Start the future
        return f.start(self.func, *args, **kwargs)


class FutureTaskDecorator(TaskDecorator):
    decorates = FutureTask


future = FutureTaskDecorator

