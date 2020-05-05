from __future__ import absolute_import

# noinspection PyPackageRequirements
import datetime

from google.appengine.api.taskqueue import taskqueue

from forq.exceptions import PermanentTaskFailure, SingularTaskFailure, TaskPermissionError
from forq.queue import Queue
from forq.store import Store
from .handlers import generate_callback_route
from forq.utils import encode_function, decode_function

# noinspection PyPackageRequirements

QUEUE_HEADERS = {"Content-Type": "application/octet-stream"}
DEFAULT_OVERFLOW_STORE_FACTORY = None


def default_overflow_store_factory():
    if DEFAULT_OVERFLOW_STORE_FACTORY:
        return DEFAULT_OVERFLOW_STORE_FACTORY
    else:
        from forq.contrib.appengine.store import AppEngineStore
        return AppEngineStore


# noinspection PyUnusedLocal
def run_from_appengine_queue(data, *args, **kwargs):
    # Check that the callback is via correct channel
    headers = kwargs.get('headers', {})
    if not headers.get('X-Appengine-TaskName', None):
        raise TaskPermissionError('AppEngine scheduled Task must be called from a Push TaskQueue')

    # Check if state was stored
    stored_state = kwargs.pop('stored_state', None)
    if stored_state:
        # Get the state from a store state
        try:
            store = Store.from_state(data)
            task = store.get('task')
        except Exception:
            raise PermanentTaskFailure("Task state is missing from store")
        func, func_args, func_kwargs = decode_function(task)
        args = func_args or []
        kwargs.update(func_kwargs or {})
    else:
        store = None
        func = data
        args = list(args)

    # Run the func
    try:
        if not callable(func):
            raise PermanentTaskFailure("Task is missing the function")
        # Call old style callback
        result = func(*args, **kwargs)
    except SingularTaskFailure:
        raise
    except Exception as err:
        raise PermanentTaskFailure(err)
    finally:
        if stored_state and store:
            store.delete()

    return result


class AppEngineQueue(Queue):
    supports = ['run_at', 'transactional', 'retry_options']

    def __init__(self, *args, **kwargs):
        self.task_queue = kwargs.pop("taskqueue", "default")
        self.transactional = kwargs.pop("transactional", False)
        self.namespace = kwargs.get("namespace", None)
        self.log_name = kwargs.pop('log_name', "")
        self._store = kwargs.pop("queue_store", None)
        kwargs.setdefault("headers", {}).update(QUEUE_HEADERS)
        kwargs["url"] = generate_callback_route("/%s" % self.log_name)

        super(AppEngineQueue, self).__init__(*args, **kwargs)

        self.task_kwargs = kwargs

    @property
    def store(self):
        if self._store is None:
            store_factory = default_overflow_store_factory()
            if store_factory is None:
                raise Exception("Store factory not available")
            k = "queue:%s" % self.key
            self._store = store_factory(key=k, namespace=self.namespace)
        return self._store

    # noinspection PyUnusedLocal
    def _schedule(self, payload, **task_kwargs):

        # Create and enqueue an AppEngine Task
        t = taskqueue.Task(payload=payload, **task_kwargs)
        task = t.add(self.task_queue, transactional=self.transactional)

        # Return created task
        return task

    def add(self, func, *args, **kwargs):

        # Allow for ETA/run_at/run_after in seconds
        task_args = self.task_kwargs
        if '_run_at' in kwargs or '_eta' in kwargs:
            task_args['eta'] = kwargs.pop("_run_at", kwargs.pop("_eta", None))
        elif '_run_after' in kwargs:
            task_args['eta'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=kwargs['_run_after'])

        # Schedule on a task queue
        # Wrap in callback (to handle include scopes etc)
        payload = encode_function(run_from_appengine_queue, func, *args, **kwargs)

        try:
            # Schedule task
            task = self._schedule(payload, **self.task_kwargs)
        except taskqueue.TaskTooLargeError:

            # If we're here the task needs to be put in a store
            store = self._store
            store.set('task', encode_function(func, *args, **kwargs))

            # Wrap in callback (to handle include scopes etc)
            payload = encode_function(run_from_appengine_queue, store.to_state(), stored_state=True)

            # Schedule on a task queue
            task = self._schedule(payload, **self.task_kwargs)

        return task

    def delete(self, task):
        if isinstance(task, basestring):
            taskqueue.Queue(self.task_queue).delete_tasks_by_name(task)
        if isinstance(task, taskqueue.Task):
            taskqueue.Queue(task.queue_name).delete_tasks(task)
