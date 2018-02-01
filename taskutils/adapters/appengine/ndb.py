from __future__ import absolute_import
# noinspection PyPackageRequirements
from google.appengine.api.taskqueue import taskqueue
# noinspection PyPackageRequirements
from google.appengine.ext.ndb import Key, Model, BlobProperty

from taskutils.adapters.appengine import AppEngineTaskAdapter, run_appengine_task
from taskutils.exceptions import PermanentTaskFailure, TaskPropertyError


class QueuedTask(Model):
    """Datastore representation of a deferred task.

    This is used in cases when the deferred task is too big to be included as
    payload with the task queue entry.
    """
    data = BlobProperty(required=True)  # up to 1 mb


DEFAULT_QUEUED_MODEL = QueuedTask
DEFAULT_QUEUED_NAMESPACE = None


def run_ndb_task(key, headers=None, request=None, **kwargs):
    if isinstance(key, Key):
        try:
            entity = key.get()
        except Exception:
            raise PermanentTaskFailure()
        if 'data' in entity:
            run_appengine_task(entity.data, headers=headers, request=request, **kwargs)


class NDBTaskAdapter(AppEngineTaskAdapter):

    def __init__(self, *args, **kwargs):
        super(NDBTaskAdapter, self).__init__(*args, **kwargs)

        # Grab some NDB info
        self.model = self.kwargs.pop("_queued_model", DEFAULT_QUEUED_MODEL)
        self.namespace = self.kwargs.pop("_queued_model_namespace", DEFAULT_QUEUED_NAMESPACE)

        # Get parent if provided
        self.parent = self.kwargs.pop("parent", None)

        # Check the parent
        if self.parent:
            # noinspection PyProtectedMember
            if not isinstance(self.parent, Key) or self.parent.kind() != self.model._get_kind():
                raise TaskPropertyError('Parent must be a key of the same kind as the tasks model')
            if self.parent.namespace() != self.namespace:
                raise TaskPropertyError('Parent namespace must be the same as the task namespace')

    def run(self, func, args, kwargs, **run_kwargs):
        try:
            task = super(NDBTaskAdapter, self).run(func, args, kwargs, **run_kwargs)
        except taskqueue.TaskTooLargeError:
            # If we're here the task needs to be encoded into a model
            # Encode function to run
            encoded_function = self.encode_function(func, *args, **kwargs)

            # Create a model ans store the large encoded function
            task_entity = self.model(data=encoded_function, parent=self.parent, namespace=self.namespace)
            task_entity.put()

            # Wrap in run_request_task (to handle include scopes etc)
            payload = self.encode_function(run_ndb_task, task_entity.key, **self.scope)

            # Schedule on a task queue
            task = self.schedule(payload, log_name=run_kwargs.pop('log_name', None))

        return task
