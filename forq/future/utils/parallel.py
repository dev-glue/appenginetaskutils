import functools
import logging

from forq.future import make_future, run_future, expires
from forq.future.utils.sequence import FutureSequence, FutureSequenceTaskDecorator, FutureSequenceTask
from forq.store import Store
from forq.utils import chunks, iterchunks, split


def make_future_parallel(store_state, queue_state, **extra_kwargs):
    return make_future(store_state, queue_state, factory=FutureParallel, **extra_kwargs)


def sequence_callback(state, sequence_id, *args, **kwargs):
    try:
        # Setup all context
        kwargs['sequence_id'] = sequence_id
        sequence = kwargs['sequence'] = kwargs.pop('future', None)
        error = kwargs['sequence_error'] = kwargs.pop('error', None)
        status = kwargs['sequence_status'] = 'error' if error else 'success'

        #  for r in iter(kwargs.pop('result', [])):
        sequence_result = list(kwargs.pop('result', []))

        # Get original future
        f = FutureParallel.from_state(state)
        sequences = f.store.get("__sequences__") or 1
        f.store.set("result_%s" % sequence_id, sequence_result)
        f.store.set("status_%s" % sequence_id, status)
        # f.store.set("error_%s" % sequence_id, error)
        # f.store.set("sequence_%s" % sequence_id, future)

        c = f.store.counter('__completed__')
        t = c.inc()
        if t >= sequences:
            result = f._result(sequences)
            # Run the future
            f.run(result, args, kwargs)
    except Exception as err:
        logging.error("Unable to handle call back")

class ParallelResult(object):
    def __init__(self, store, limit, batch_size):
        self.store = store
        self.limit = limit
        self.batch_size = batch_size or 1

    def __iter__(self):
        # get list of indexes
        for index in ('result_%s' % i for i in xrange(self.limit)):
            sequence_results = self.store.get(index)
            for results in iterchunks(sequence_results, self.batch_size):
                for result in results:
                    yield result


def collate_results(*args, **kwargs):
    print args, kwargs


class FutureParallel(FutureSequence):
    """ Similar to map but uses locks to manage concurrency """
    def __init__(self, *args, **kwargs):
        self.concurrency = kwargs.pop("concurrency", None)

        # Setup Future
        super(FutureParallel, self).__init__(*args, **kwargs)

    @classmethod
    def constructor(cls):
        return make_future_parallel

    def start(self, func, *args, **kwargs):
        # Collate parts
        parts = [items for items in split(self.items, self.concurrency)]
        self.store.set("__sequences__", len(parts))

        # Save the initial Future object to its store so it can be recovered
        self.save()
        state = self.to_state()

        task = None
        for sequence_id, items in enumerate(parts):

            # Create a callback
            callback = functools.partial(sequence_callback, state, sequence_id)

            # Create the FutureSequenceTask of the split
            ft = FutureSequenceTask(
                func=func,
                items=items,
                always=callback,
                result_batch_size=self.result_batch_size,
                batch_size=self.batch_size)
            if task is None:
                task = ft
            p = ft()  # Start the task

        # Enqueue the Future 'run function'
        timeout = kwargs.pop("timeout", None)
        if timeout:
            # Handle a timeout
            if self.queue and 'run_at' in self.queue.supports:
                self.enqueue(run_at=expires(timeout))

        # Returns the 1st task sequence
        return p

    def _result(self, item_count):
        return ParallelResult(self.store, item_count, self.batch_size)


class FutureParallelTask(FutureSequenceTask):
    _factory = FutureParallel


class FutureParallelTaskDecorator(FutureSequenceTaskDecorator):
    decorates = FutureParallelTask


parallel = FutureParallelTaskDecorator
