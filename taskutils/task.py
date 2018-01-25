import functools
import pickle
from copy import deepcopy

import cloudpickle
import webapp2
from google.appengine.api import taskqueue
from google.appengine.ext import ndb
from google.appengine.ext import webapp

from taskutils.util import logdebug, logwarning, logexception, dumper, get_dump

TASKUTILS_TASKROUTE = "/_ah/task"


def set_taskroute(value):
    global TASKUTILS_TASKROUTE
    TASKUTILS_TASKROUTE = value


def get_taskroute():
    global TASKUTILS_TASKROUTE
    return TASKUTILS_TASKROUTE


def get_webapp_url():
    return "%s/(.*)" % get_taskroute()


def get_enqueue_url(suffix):
    return "%s/%s" % (get_taskroute(), suffix)


# _DEFAULT_WEBAPP_URL = "/_ah/task/(.*)"
# _DEFAULT_ENQUEUE_URL = "/_ah/task/%s"

_TASKQUEUE_HEADERS = {"Content-Type": "application/octet-stream"}


class Error(Exception):
    """Base class for exceptions in this module."""


class PermanentTaskFailure(Error):
    """Indicates that a task failed, and will never succeed."""


class RetryTaskException(Error):
    """Indicates that task needs to be retried."""


class _TaskToRun(ndb.Model):
    """Datastore representation of a deferred task.
    
    This is used in cases when the deferred task is too big to be included as
    payload with the task queue entry.
    """
    data = ndb.BlobProperty(required=True)  # up to 1 mb


def _run(data, headers):
    """Unpickles and executes a task.
    
    Args:
      data: A pickled tuple of (function, args, kwargs) to execute.
    Returns:
      The return value of the function invocation.
    """
    try:
        func, args, kwargs, extra = pickle.loads(data)
    except Exception, e:
        raise PermanentTaskFailure(e)
    else:
        if extra.get("_run_from_datastore"):
            _run_from_datastore(headers, args[0])
        else:
            if extra.get("includeheaders"):
                kwargs["headers"] = headers
            func(*args, **kwargs)


def _run_from_datastore(headers, key):
    """Retrieves a task from the datastore and executes it.
    
    Args:
      key: The datastore key of a _DeferredTaskEntity storing the task.
    Returns:
      The return value of the function invocation.
    """
    logwarning("running task from datastore")
    entity = key.get() if key and isinstance(key, ndb.Key) else None
    if entity:
        try:
            _run(entity.data, headers)
        except PermanentTaskFailure:
            key.delete()
            entity._deleted = True
            raise
        else:
            key.delete()
            entity._deleted = True


def task(f=None, **kw):
    if not f:
        return functools.partial(task, **kw)

    task_kwargs = deepcopy(kw)

    queue = task_kwargs.pop("queue", "default")
    transactional = task_kwargs.pop("transactional", False)
    parent = task_kwargs.pop("parent", None)
    include_headers = task_kwargs.pop("includeheaders", False)
    log_name = task_kwargs.pop("logname", "%s/%s" % (getattr(f, '__module__', 'none'), getattr(f, '__name__', 'none')))

    task_kwargs["headers"] = dict(_TASKQUEUE_HEADERS)

    url = get_enqueue_url(log_name)  # _DEFAULT_ENQUEUE_URL % logname

    task_kwargs["url"] = url.lower()

    logdebug(task_kwargs)

    extra = {"includeheaders": include_headers}

    @functools.wraps(f)
    def run_task(*args, **kwargs):
        pickled = cloudpickle.dumps((f, args, kwargs, extra))
        logdebug("task pickle length: %s" % len(pickled))
        if get_dump():
            logdebug("f:")
            dumper(f)
            logdebug("args:")
            dumper(args)
            logdebug("kwargs:")
            dumper(kwargs)
            logdebug("extra:")
            dumper(extra)
        try:
            t = taskqueue.Task(payload=pickled, **task_kwargs)
            return t.add(queue, transactional=transactional)
        except taskqueue.TaskTooLargeError:
            if parent:
                key = _TaskToRun(data=pickled, parent=parent).put()
            else:
                key = _TaskToRun(data=pickled).put()
            ds_pickled = cloudpickle.dumps((None, [key], {}, {"_run_from_datastore": True}))
            t = taskqueue.Task(payload=ds_pickled, **task_kwargs)
            return t.add(queue, transactional=transactional)

    return run_task


def isFromTaskQueue(headers):
    """ Check if we are currently running from a task queue """
    # As stated in the doc (https://developers.google.com/appengine/docs/python/taskqueue/overview-push#Task_Request_Headers)
    # These headers are set internally by Google App Engine.
    # If your request handler finds any of these headers, it can trust that the request is a Task Queue request.
    # If any of the above headers are present in an external user request to your App, they are stripped.
    # The exception being requests from logged in administrators of the application, who are allowed to set the headers for testing purposes.
    return bool(headers.get('X-Appengine-TaskName'))


# Queue & task name are already set in the request log.
# We don't care about country and name-space.
_SKIP_HEADERS = {'x-appengine-country', 'x-appengine-queuename', 'x-appengine-taskname',
                 'x-appengine-current-namespace'}


def _launch_task(pickled, name, headers):
    try:
        # Add some task debug information.
        #         dheaders = []
        #         for key, value in headers.items():
        #             k = key.lower()
        #             if k.startswith("x-appengine-") and k not in _SKIP_HEADERS:
        #                 dheaders.append("%s:%s" % (key, value))
        #         logdebug(", ".join(dheaders))
        logdebug(", ".join(["%s:%s" % (key, value) for key, value in headers.items()]))

        if not isFromTaskQueue(headers):
            raise PermanentTaskFailure('Detected an attempted XSRF attack: we are not executing from a task queue.')

        logdebug('before run "%s"' % name)
        _run(pickled, headers)
        logdebug('after run "%s"' % name)
    except PermanentTaskFailure:
        logexception("Aborting task")
    except:
        logexception("failure")
        raise
    # else let exceptions escape and cause a retry


class TaskHandler(webapp.RequestHandler):
    def post(self, name):
        _launch_task(self.request.body, name, self.request.headers)


class TaskHandler2(webapp2.RequestHandler):
    def post(self, name):
        _launch_task(self.request.body, name, self.request.headers)


def addrouteforwebapp(routes):
    routes.append((get_webapp_url(), TaskHandler))


#     routes.append((_DEFAULT_WEBAPP_URL, TaskHandler))

def addrouteforwebapp2(routes):
    routes.append((get_webapp_url(), TaskHandler2))
#     routes.append((_DEFAULT_WEBAPP_URL, TaskHandler2))
