from __future__ import absolute_import

import functools

from copy import deepcopy

from taskutils.utils import encode_function

DEFAULT_TASK_ADAPTER = None


def default_adapter_factory():
    if DEFAULT_TASK_ADAPTER:
        return DEFAULT_TASK_ADAPTER
    else:
        from taskutils.adapters.appengine.ndb import NDBTaskAdapter
        return NDBTaskAdapter


class Task(object):
    """
    Decorator allows for passing zero or more parameters (or none to adapters)
    usage

    @Task
    def func(*args):
      print args

    func(1)
    # Prints [1] when after task is scheduled by default adapter and run

    or

    @Task(1,2,3,kw=1)
    def func(*args):
      print args

    func(1)
    # Prints [1] when after task is scheduled by adapter and run (adapter is passed 1,2,3,kw=1)

    or

    def func(*args):
      print args

    task = Task(1,2,3,kw=1)
    task_func = task(func)
    task_func(1)
    # Prints [1] when after task is scheduled by adapter and run (adapter is passed 1,2,3,kw=1)

    or

    task1 = Task(1,2,3,kw=1)
    task2 = task1(4,5,6,kw2=2)
    task_func = task2(func)
    task_func(1)
    # Prints [1] when after task is scheduled by adapter and run (adapter is passed 1,2,3,4,5,6,kw=1,kw=2)

    """
    def __init__(self, *args, **kwargs):

        # Check if a zero parameter decorator
        if len(kwargs) == 0 and len(args) == 1 and callable(args[0]):
            self.func = args[0]

        self.args = args
        self.kwargs = kwargs

    def __call__(self, *args, **kwargs):
        self._call(*args, **kwargs)

    def _call(self, *args, **kwargs):
        if len(args) == 1 and callable(args[0]):
            self.func = args[0]
        else:
            self.args += args
            self.kwargs.update(kwargs)

        if self.func:
            return self._wrap_function()
        else:
            return self._call

    def _wrap_function(self):

        task_kwargs = deepcopy(self.kwargs)
        log_name = task_kwargs.pop("log_name", None)
        if not log_name:
            log_name = "%s/%s" % (getattr(self.func, '__module__', 'none'), getattr(self.func, '__name__', 'none'))

        # Get a task adapter
        adapter = task_kwargs.pop('adapter', None)

        # Create an adapter from the factory
        if adapter is None:
            adapter_factory = task_kwargs.pop('adapter_factory', None) or default_adapter_factory()
            adapter = adapter_factory(*self.args, **self.kwargs)

        func = self.func

        @functools.wrap(func)
        def task_(*args, **kwargs):
            encoded_function = encode_function(func, *args, **kwargs)
            # Using adapter (one already created with tasks kwargs) or use adapter_factory or the default adapter factory
            adapter.run(encoded_function, log_name=log_name)

        return task_


task = Task
