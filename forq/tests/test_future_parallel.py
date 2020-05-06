import datetime
import os
import random
import sys
import time
from collections import Counter
from multiprocessing import Manager, Pipe, Process, cpu_count

from forq.future.utils.parallel import parallel, FutureParallelTask
from forq.queue.asynchronous import ProcessQueue
from forq.store.dictionary import DictStore


def _background_store(storage):
    # Wait till storage is no longer __running__

    while storage.get('__running__'):
        time.sleep(0.1)


def background_storage_process():
    manager = Manager()
    storage = manager.dict(__running__=True)

    process = Process(target=_background_store, args=(storage,))
    process.start()

    return storage, process


def compare(s, t):
    return Counter(s) == Counter(t)

def test_future_parallel():

    def async_task(item, *args, **kwargs):
        # print "\nASYNC PARALLEL"
        # print"\nARGS:%s\nKWARGS:%s" % (args, kwargs)
        # print "Item %s (%s of %s) at %s" % (item, index+1, len(items), datetime.datetime.utcnow())
        return item * item

    def success(*args, **kwargs):
        # print "\nSUCCESS"
        # print"\nARGS:%s\nKWARGS:%s" % (args, kwargs)
        result = kwargs.get('result')
        # print list(result)
        pf = kwargs.get('future')
        if pf:
            # Short circuit standard clear
            def noop(*args, **kwargs):
                pass
            pf.store.clear = noop
            pf.store.set('result', list(result))

    def always(*args, **kwargs):
        # print "\nALWAYS"
        # print"\nARGS:%s\nKWARGS:%s" % (args, kwargs)
        pf = kwargs.get('future')
        if pf:
            pf.store.storage['__running__'] = False

    storage, storage_process = background_storage_process()
    cpus = cpu_count()
    queue = ProcessQueue(processes=cpus)
    store = DictStore(key="root", storage=storage)

    items = range(3000)
    r = [async_task(i) for i in items]
    f = parallel(async_task, items=items, store=store, concurrency=cpus, queue=queue)  # type: FutureParallelTask
    f.success(success).always(always)

    f()

    # Wait for the store to complete
    storage_process.join(timeout=300)
    if storage_process.is_alive():
        storage_process.terminate()

    async_r = store.get('result')
    assert compare(r, async_r)

    if queue.pool:
        queue.pool.terminate()
        queue.pool.join()

    print "\n%s" % r
