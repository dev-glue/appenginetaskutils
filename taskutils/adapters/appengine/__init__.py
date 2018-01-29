from __future__ import absolute_import
# noinspection PyPackageRequirements
from copy import deepcopy
from google.appengine.api.taskqueue import taskqueue
from taskutils.adapters import TaskAdapter
from taskutils.exceptions import PermanentTaskFailure, SingularTaskFailure
from taskutils.task import decode_task
from taskutils.utils import decode_function, encode_function

QUEUE_HEADERS = {"Content-Type": "application/octet-stream"}
DEFAULT_TASK_ROUTE = "/_ah/task"


def set_default_route(route):
    global DEFAULT_TASK_ROUTE
    DEFAULT_TASK_ROUTE = route


def get_default_route():
    return DEFAULT_TASK_ROUTE


def _run_task(data, headers=None, request=None, **kwargs):
    """Unpickles and executes a task.

    Args:
      data: An encoded task data
      headers: A dict of request headers (optional)
      request: The original request
    Returns:
      The return value of the function invocation.

    """
    extras = kwargs
    try:
        func, args, kwargs = decode_function(data)
    except Exception as err:
        raise PermanentTaskFailure(err)
    else:
        if isinstance(extras, dict):
            if extras.pop("include_headers", None):
                kwargs["headers"] = headers
            if extras.pop("include_request", None):
                kwargs["request"] = request
            if extras.pop("include_extras", None):
                kwargs["extras"] = extras
        try:
            func(*args, **kwargs)
        except SingularTaskFailure:
            raise
        except PermanentTaskFailure:
            raise
        except Exception as err:
            raise PermanentTaskFailure(err)


class AppEngineTaskAdapter(TaskAdapter):
    # Get base callback url
    base_route = get_default_route()

    def __init__(self, *args, **kwargs):
        super(AppEngineTaskAdapter, self).__init__(*args, **kwargs)

        # Get queue and transactional props
        self.queue = kwargs.pop("queue", "default")
        self.transactional = kwargs.pop("transactional", False)

        headers = self.kwargs.setdefault("headers", {})
        headers.update(QUEUE_HEADERS)

        # Get extras (passed to unpickle functions)
        extras = self.kwargs.pop("extras", None)
        extras = extras if isinstance(extras, dict) else {}

        # Update extras with kw args
        extras.update({k: self.kwargs.pop(k) for k, v in self.kwargs.iteritems() if
                       k in ['include_headers', 'include_request', 'include_extras'] and v})
        self.extras = extras

    def task_options(self, log_name=None):
        task_kwargs = deepcopy(self.kwargs)
        task_kwargs["url"] = ("%s%s" % (self.base_route, "/%s" % log_name if log_name else "")).lower()
        return task_kwargs

    def run_task(self, encoded_function, log_name=None):

        # Wrap in runner
        payload = encode_function(_run_task, encoded_function, **self.extras)

        # Enqueue the task
        t = taskqueue.Task(payload=payload, **self.task_options(log_name=log_name))
        task = t.add(self.queue, transactional=self.transactional)

        return task
