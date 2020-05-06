import functools
from itertools import islice

from forq.future import FutureTask, FutureTaskDecorator, Future, make_future, ContinueFuture
from forq.store import Store
from forq.utils import encode_function, iterchunks, decode_function


def make_future_sequence(store_state, queue_state, **extra_kwargs):
    return make_future(store_state, queue_state, factory=FutureSequence, **extra_kwargs)


class SequenceResult(object):
    def __init__(self, store, limit, batch_size):
        self.store = store
        self.limit = limit
        self.batch_size = batch_size or 1

    def __iter__(self):
        # get list of indexes
        for indexes in iterchunks(('result_%s' % i for i in xrange(self.limit)), self.batch_size):
            results = self.store.get_many(indexes)
            for result in results:
                yield result


class FutureSequence(Future):
    index = 0
    count = 0

    def __init__(self, *args, **kwargs):
        # Get items and the index (items is usually passed in on creation)
        self.items = kwargs.pop("items", None)

        # Get som useful optimisers
        self.result_batch_size = kwargs.pop("result_batch_size", None)
        self.batch_size = kwargs.pop("batch_size", 1)

        super(FutureSequence, self).__init__(*args, **kwargs)

        # At this point we should have a valid store to get items if not passed in
        if self.items is None:
            self.items = self.store.get('items') or []

        # Update length
        self.length = kwargs.pop("length", 0) or len(self.items) if self.items else 0

    @classmethod
    def constructor(cls):
        return make_future_sequence

    def to_dict(self):
        d = super(FutureSequence, self).to_dict()
        d.pop('items', None)  # Don't include items as that is held in a store
        return d

    def save(self):
        super(FutureSequence, self).save()
        # Store items in store
        self.store.set('items', self.items)

    def _retry_counter(self):
        # Check for retries and timeouts
        return self.store.counter('retries_%s' % self.index)

    def _result(self, item_count):
        return SequenceResult(self.store, item_count, self.batch_size)

    def _func(self, func, *args, **kwargs):
        index = kwargs.pop('_sequence_index', 0) or 0
        limit = kwargs.pop('_sequence_limit', None) or self.length
        if index < limit:
            # Get items from store
            items = self.store.get('items')
            # Enumerate over items in a batch size chunk
            for count, item in enumerate(islice(items, index, index + self.batch_size)):

                # Get current item index
                current = index + count

                # Process index item
                try:
                    c = kwargs.pop('_sequence_continue', None)
                    if c:
                        cf, ca, ckw = decode_function(c)
                        if callable(cf):
                            _, result = cf(*ca, **ckw)
                        else:
                            result = c
                    else:
                        _, result = super(FutureSequence, self)._func(func, item, index, items, *args, **kwargs)
                except ContinueFuture as cf:

                    if cf.func != func and callable(cf.func):
                        kwargs['_sequence_continue'] = encode_function(cf.func, *cf.args, **cf.kwargs)  # Original func is always preserved in sequence
                    # We stop the batch here and let the index < limit trigger a future function from the same point on
                    kwargs['_sequence_index'] = index
                    kwargs['_sequence_limit'] = limit
                    raise ContinueFuture(func, *args, **kwargs)
                except Exception as er:
                    raise er
                self.store.set('result_%s' % current, result)
                index = current + 1

        # Work out what based on index
        if index < limit:
            kwargs['_sequence_index'] = index
            kwargs['_sequence_limit'] = limit
            # Schedule another future call if required
            raise ContinueFuture(func, *args, **kwargs)
        else:
            # All done? Or check for errors
            completed = True
            result = self._result(limit)  # Create generator result-set

        return completed, result


class FutureSequenceTask(FutureTask):
    _factory = FutureSequence

    def __init__(self, *args, **kwargs):
        # Pop off sequence of items and convert it into a list

        # Setup Future
        super(FutureSequenceTask, self).__init__(*args, **kwargs)


class FutureSequenceTaskDecorator(FutureTaskDecorator):
    decorates = FutureSequenceTask


sequence = FutureSequenceTaskDecorator
