import uuid

from forq.exceptions import TaskContextError, TaskNotImplementedError
from forq.utils import decode_function


class BaseQueue(object):
    supports = []

    def __init__(self, *args, **kwargs):
        pass

    # noinspection PyUnusedLocal
    def add(self, func, *args, **kwargs):
        raise TaskNotImplementedError()

    # noinspection PyUnusedLocal
    def delete(self, task_id):
        raise TaskNotImplementedError()

    def factory(self):
        return self.__class__

    def to_state(self):
        raise TaskNotImplementedError()

    @classmethod
    def from_state(cls, state):
        # noinspection PyBroadException
        func, args, kwargs = decode_function(state)
        if func:
            # noinspection PyUnboundLocalVariable
            return func(*args, **kwargs)


class Queue(BaseQueue):
    """
        The simple TaskAdapter doesn't encode functions nor does it route or handle routes it simply runs the task

    """

    def __init__(self, *args, **kwargs):
        self.key = kwargs.pop('key', uuid.uuid4().get_hex())

        super(Queue, self).__init__(*args, **kwargs)

    def add(self, func, *args, **kwargs):
        if callable(func):
            func(*args, **kwargs)  # TODO make this run async




# TODO: Threaded queue
DEFAULT_QUEUE = None


def default_queue_factory():
    if DEFAULT_QUEUE:
        return DEFAULT_QUEUE
    else:
        try:
            from forq.contrib.appengine.queue import AppEngineQueue as DefaultQueue
        except ImportError:
            try:
                from forq.queue.asynchronous import ProcessQueue as DefaultQueue
            except ImportError:
                raise Exception("No queue factory is available for use")

        return DefaultQueue
