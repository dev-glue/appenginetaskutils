import os
import sys
import uuid
import datetime
from multiprocessing import Process, log_to_stderr, Manager, Pool, cpu_count
import time
from forq.exceptions import PermanentTaskFailure
from forq.queue import Queue
from forq.utils import encode_function, decode_function


_process_queue = None
_process_pool = None
_process_manager = None


def make_process_queue(key=None, *args, **kwargs):
    return ProcessQueue(key=key, *args, **kwargs)


# noinspection PyUnusedLocal
def run_from_multiprocessing_queue(func, *args, **kwargs):
    # Run the func
    try:
        if not callable(func):
            raise PermanentTaskFailure("Missing the function")
        # Call old style callback
        result = func(*args, **kwargs)
    except Exception as err:
        raise PermanentTaskFailure(err)
    finally:
        pass

    return result


def run_process(eta, data):
    if eta:
        now = datetime.datetime.utcnow()
        if now < eta:
            time.sleep((eta-now).seconds)

    # Run function
    try:
        func, args, kwargs = decode_function(data)
    except Exception as err:
        raise PermanentTaskFailure(err)
    if callable(func):
        return func(*args, **kwargs)
    else:
        raise PermanentTaskFailure("Missing function")


def run_queue(queue):
    # print '\n', os.getpid(), " waiting", datetime.datetime.now()
    global _process_queue
    _process_queue = queue

    while True:
        args = queue.get(True)
        if args:
            # print '\n', os.getpid(), " running"

            run_process(*args)
        else:
            # print '\n', os.getpid(), " sleeping"
            time.sleep(1)  # simulate a "long" operation

    # print '\n', os.getpid(), " done"


def get_global_pool(processes=None):
    global _process_queue
    global _process_pool

    if _process_queue is None:
        global _process_manager
        _process_manager = Manager()
        _process_queue = _process_manager.Queue()
        _process_pool = Pool(processes or cpu_count(), run_queue, (_process_queue,))
    return _process_pool, _process_queue


class ProcessQueue(Queue):
    """
        The simple TaskAdapter doesn't encode functions nor does it route or handle routes it simply runs the task

    """
    pool = None

    def __init__(self, *args, **kwargs):
        self.key = kwargs.pop('key', uuid.uuid4().get_hex())
        self.processes = kwargs.pop('processes', None)
        self.queue = kwargs.pop('queue', None)
        if not self.queue:
            self.pool, self.queue = get_global_pool(self.processes)

        super(Queue, self).__init__(*args, **kwargs)

    def to_state(self):
        return encode_function(make_process_queue, key=self.key, queue=self.queue)

    def add(self, func, *args, **kwargs):

        # Allow for ETA/run_at/run_after in seconds
        eta = None
        if '_run_at' in kwargs or '_eta' in kwargs:
            eta = kwargs.pop("_run_at", kwargs.pop("_eta", None))
        elif '_run_after' in kwargs:
            eta = datetime.datetime.utcnow() + datetime.timedelta(seconds=kwargs['_run_after'])

        # Schedule on a task queue
        payload = encode_function(run_from_multiprocessing_queue, func, *args, **kwargs)
        self.queue.put((eta, payload, ))

