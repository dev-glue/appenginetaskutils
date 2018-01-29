from __future__ import absolute_import

import functools
from taskutils.utils import encode_function


def default_adapter_factory():
    from taskutils.adapters.appengine.ndb import NDBTaskAdapter
    return NDBTaskAdapter


def task(func=None, adapter_factory=None, adapter=None, **task_kwargs):
    if not func:
        return functools.partial(task, adapter_factory=adapter_factory, adapter=adapter, **task_kwargs)

    # Get adapter (one already created with tasks kwargs) or use adapter_factory or the default adapter factory
    if adapter is None:
        adapter_factory = adapter_factory or default_adapter_factory()
        adapter = adapter_factory(**task_kwargs)

    log_name = task_kwargs.pop("log_name", "%s/%s" % (getattr(func, '__module__', 'none'), getattr(func, '__name__', 'none')))

    @functools.wraps(func)
    def task_(*args, **kwargs):
        encoded_function = encode_function(func, *args, **kwargs)
        adapter.run_task(encoded_function, log_name=log_name)

    return task_


