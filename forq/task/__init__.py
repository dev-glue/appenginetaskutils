from __future__ import absolute_import

import functools
import uuid

from forq.exceptions import PermanentTaskFailure
from forq.queue import default_queue_factory
from forq.utils import encode_function, decode_function, hash_id

DEFAULT_TASK_QUEUE = None


def run_encoded_task_with_context(encoded_task, *context_args, **context):
    func, args, kwargs = decode_function(encoded_task)
    if func is None:
        raise PermanentTaskFailure("Couldn't decode task")

    # Always include task context when run unless specifically told not to
    kwargs['context'] = context
    kwargs['context_args'] = context_args

    # noinspection PyUnboundLocalVariable
    results = func(*args, **kwargs)

    return results


def run_encoded_task(encoded_task, *context_args, **context_kwargs):
    func, args, kwargs = decode_function(encoded_task)
    if func is None:
        raise PermanentTaskFailure("Couldn't decode task")

    # noinspection PyUnboundLocalVariable
    results = func(*args, **kwargs)

    return results


class Task(object):
    """
    Decorator allows for passing zero or more parameters (or none to adapters)
    usage

    @Task
    def func(*args):
      print args

    func(1)
    # Prints [1] after task is scheduled and run by default queue

    or

    @Task(1,2,3,kw=1)
    def func(*args):
      print args

    func(1)
    # Prints [1] after task is scheduled and run by default queue (queue is passed 1,2,3,kw=1)

    or

    def func(*args):
      print args

    task = Task(1,2,3,kw=1)
    task_func = task(func)
    task_func(1)
    # Prints [1] after task is scheduled and run by default queue (queue is passed 1,2,3,kw=1)

    or

    task1 = Task(1,2,3,kw=1)
    task2 = task1(4,5,6,kw2=2)
    task_func = task2(func)
    task_func(1)
    # Prints [1] after task is scheduled and run by default queue (queue is passed 1,2,3,4,5,6,kw=1,kw=2)

    Task decorator kwargs:
      queue - a way to override the default queue system
      include_context - whether to include context from the callback/run execeution

    Shred kwargs from queue
      name - name of the task/queue
      key - key for the task/queue (generated if queue key is None)

    All other args and kwargs are passed to underlying Queue

    """
    func = None
    task = None
    name = None
    key = None

    _args = None
    _kwargs = None
    
    def __init__(self, func=None, *args, **kwargs):
        self.func = func

        # Create a log name (can be used by adapters)
        if 'log_name' not in kwargs:
            self.log_name = "%s/%s" % (getattr(self.func, '__module__', 'none'), getattr(self.func, '__name__', 'none'))
            kwargs["log_name"] = self.log_name

        # Get task props
        self._queue = kwargs.pop("queue", DEFAULT_TASK_QUEUE)  # Get queue if supplied
        self.include_context = kwargs.pop("include_context", False)  # Determines is run encoded task includes context

        # Get name of task
        self.name = kwargs.get('name', None)

        # Build a key (share/use the queue key if provided)
        key = kwargs.get('key', None)
        self.key = key or hash_id(self.name) if self.name else uuid.uuid4().get_hex()

        # Store rest of args and kwargs
        self._args = list(args)
        self._kwargs = kwargs

    @property
    def queue(self):
        if not self._queue:
            factory = default_queue_factory()
            self._queue = factory(*self._args, **self._kwargs)
        return self._queue

    def __call__(self, *args, **kwargs):
        return self._task(*args, **kwargs)

    def _task(self, *args, **kwargs):

        # Encode function
        encoded_task = encode_function(self.func, *args, **kwargs)

        # Get Queue
        queue = self.queue

        # Add encoded task to be run by run_encoded_task once coming off the queue
        if self.include_context:
            queue.add(run_encoded_task_with_context, encoded_task, *self._args, **self._kwargs)
        else:
            queue.add(run_encoded_task, encoded_task)


class TaskDecorator(object):

    func = None
    decorates = Task

    @classmethod
    def _extract_func(cls, *args, **kwargs):
        # Get func if passed in either as a single arg or func keyword
        func = kwargs.pop('func', None)

        if len(args) >= 1 and callable(args[0]):
            func = args[0]
            args = args[1:]

        return func, args, kwargs,

    def __new__(cls, *args, **kwargs):
        func, _args, _kwargs = cls._extract_func(*args, **kwargs)
        if func:
            return cls.decorates(func, *_args, **_kwargs)
        else:
            return super(cls, cls).__new__(cls, *args, **kwargs)

    def __init__(self, *args, **kwargs):
        self.func, self._args, self._kwargs = self._extract_func(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        if not callable(self.func):
            self.func, _args, _kwargs = self._extract_func(*args, **kwargs)
            self._args += _args
            self._kwargs.update(_kwargs)

        if callable(self.func):
            return self.decorates(self.func, *self._args, **self._kwargs)
        else:
            return self.__call__


task = TaskDecorator
