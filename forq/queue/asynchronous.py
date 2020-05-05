import uuid
import datetime
from multiprocessing import Process, log_to_stderr
import time
from forq.exceptions import PermanentTaskFailure
from forq.queue import Queue
from forq.utils import encode_function, decode_function


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


class ProcessQueue(Queue):
    """
        The simple TaskAdapter doesn't encode functions nor does it route or handle routes it simply runs the task

    """
    def __init__(self, *args, **kwargs):
        self.key = kwargs.pop('key', uuid.uuid4().get_hex())
        super(Queue, self).__init__(*args, **kwargs)

    def add(self, func, *args, **kwargs):

        # Allow for ETA/run_at/run_after in seconds
        eta = None
        if '_run_at' in kwargs or '_eta' in kwargs:
            eta = kwargs.pop("_run_at", kwargs.pop("_eta", None))
        elif '_run_after' in kwargs:
            eta = datetime.datetime.utcnow() + datetime.timedelta(seconds=kwargs['_run_after'])

        # Schedule on a task queue
        payload = encode_function(run_from_multiprocessing_queue, func, *args, **kwargs)
        p = Process(target=run_process, args=[eta, payload])
        p.start()
        return p
