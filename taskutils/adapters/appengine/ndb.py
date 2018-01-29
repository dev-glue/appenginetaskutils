from __future__ import absolute_import
# noinspection PyPackageRequirements
from google.appengine.api.taskqueue import taskqueue
# noinspection PyPackageRequirements
from google.appengine.ext.ndb import Key, Model, BlobProperty

from taskutils.adapters.appengine import AppEngineTaskAdapter, _run_task
from taskutils.exceptions import PermanentTaskFailure
from taskutils.utils import encode_function


class QueuedTask(Model):
    """Datastore representation of a deferred task.

    This is used in cases when the deferred task is too big to be included as
    payload with the task queue entry.
    """
    data = BlobProperty(required=True)  # up to 1 mb


DEFAULT_QUEUED_MODEL = QueuedTask
DEFAULT_QUEUED_NAMESPACE = None


def _run_task_from_ndb(key, headers=None, request=None, **kwargs):
    if isinstance(key, Key):
        try:
            entity = key.get()
        except Exception:
            raise PermanentTaskFailure()
        if 'data' in entity:
            _run_task(entity.data, headers=headers, request=request, **kwargs)


class NDBTaskAdapter(AppEngineTaskAdapter):

    def __init__(self, *args, **kwargs):
        self.model = kwargs.pop("_queued_model", DEFAULT_QUEUED_MODEL)
        self.namespace = kwargs.pop("_queued_model_namespace", DEFAULT_QUEUED_NAMESPACE)
        super(NDBTaskAdapter, self).__init__(*args, **kwargs)

    def run_task(self, encoded_function, log_name=None):
        try:
            task = super(NDBTaskAdapter, self).run_task(encode_function, log_name=log_name)
        except taskqueue.TaskTooLargeError:
            # If we're here the task needs to be encoded into a model

            # Get optional parent of task
            parent = self.kwargs.pop("parent", None)
            # noinspection PyProtectedMember
            parent = parent if isinstance(parent, Key) and parent.kind() == self.model._get_kind() else None

            # Create a task model
            task_entity = self.model(data=encoded_function, parent=parent, namespace=self.namespace)
            task_entity.put()

            # Wrap in runner
            payload = encode_function(_run_task_from_ndb, task_entity.key, **self.extras)

            # Enqueue the task
            t = taskqueue.Task(payload=payload, **self.task_options(log_name=log_name))
            task = t.add(self.queue, transactional=self.transactional)
        return task
