from __future__ import absolute_import
from copy import deepcopy
# noinspection PyPackageRequirements
from google.appengine.api.taskqueue import taskqueue
from taskutils.adapters import TaskAdapter, run_task
from taskutils.exceptions import PermanentTaskFailure, SingularTaskFailure, TaskPermissionError
from taskutils.utils import decode_function, encode_function

QUEUE_HEADERS = {"Content-Type": "application/octet-stream"}
DEFAULT_TASK_ROUTE = "/_ah/task"


def get_default_route():
    return DEFAULT_TASK_ROUTE


def run_appengine_task(data, headers=None, request=None, **task_kwargs):
    """Unpickles and executes a task.

    Args:
      data: An encoded task data
      headers: A dict of request headers (optional)
      request: The original request
    Returns:
      The return value of the function invocation.

    """
    try:
        func, args, kwargs = decode_function(data)
    except Exception as err:
        raise PermanentTaskFailure(err)
    else:
        if task_kwargs.pop("include_headers", None):
            kwargs["headers"] = headers
        try:
            result = run_task(func, args, kwargs)
        except SingularTaskFailure:
            raise
        except PermanentTaskFailure:
            raise
        except Exception as err:
            raise PermanentTaskFailure(err)

        return result


class AppEngineTaskAdapter(TaskAdapter):

    def __init__(self, *args, **kwargs):
        super(AppEngineTaskAdapter, self).__init__(*args, **kwargs)

        # Get queue and transactional props
        self.queue = self.kwargs.pop("queue", "default")
        self.transactional = self.kwargs.pop("transactional", False)
        headers = self.kwargs.setdefault("headers", {})
        headers.update(QUEUE_HEADERS)

        # Update extras with kw args
        if self.kwargs.pop('include_headers', None):
            self.scope['include_headers'] = True

    def schedule(self, payload, log_name=None):
        # Enqueue the task
        task_kwargs = deepcopy(self.kwargs)
        task_kwargs["url"] = ("%s%s" % (self.base_route, "/%s" % log_name if log_name else "")).lower()
        t = taskqueue.Task(payload=payload, **task_kwargs)
        task = t.add(self.queue, transactional=self.transactional)
        return task

    def run(self, func, args, kwargs, **run_kwargs):

        # Encode function to run
        encoded_function = self.encode_function(func, *args, **kwargs)

        # Wrap in run_request_task (to handle include scopes etc)
        payload = self.encode_function(run_appengine_task, encoded_function, **self.scope)

        # Schedule on a task queue
        return self.schedule(payload, log_name=run_kwargs.pop('log_name', None))

    @staticmethod
    def encode_function(func, *args, **kwargs):
        return encode_function(func, *args, **kwargs)

    @staticmethod
    def callback(data, headers=None, **kwargs):
        headers = headers or {}
        if not headers.get('X-Appengine-TaskName', None):
            raise TaskPermissionError('AppEngine scheduled Task must be called from a Push TaskQueue')
        return run_appengine_task(data, **kwargs)

    @staticmethod
    def base_route():
        return get_default_route()

    @staticmethod
    def callback_route():
        return "%s/(.*)" % get_default_route()


