import datetime
import random
import time
from multiprocessing import Manager, Pipe, Process

from forq.future.utils.parallel import parallel, FutureParallelTask
from forq.store.dictionary import DictStore


def _background_store(conn):
    manager = Manager()
    multiprocess_storage = manager.dict(__running__=True)
    conn.send(multiprocess_storage)

    # Wait till storage is no longer __running__
    while multiprocess_storage.get('__running__'):
        time.sleep(1)

    conn.close()


def background_store():

    parent_conn, child_conn = Pipe()
    p = Process(target=_background_store, args=(child_conn,))
    p.start()
    storage = parent_conn.recv()
    parent_conn.close()
    store = DictStore(key="root", storage=storage)
    store.process = p
    return store


def test_future_parallel():

    def async_task(item, index, items, *args, **kwargs):
        time.sleep(random.randrange(2, 10))
        print "\nASYNC PARALLEL"
        # print"\nARGS:%s\nKWARGS:%s" % (args, kwargs)
        print "Item %s at %s" % (item, datetime.datetime.utcnow())
        return item * item

    def success(*args, **kwargs):
        print "\nSUCCESS"
        # print"\nARGS:%s\nKWARGS:%s" % (args, kwargs)
        result = kwargs.get('result')
        print list(result)

    def always(*args, **kwargs):
        print "\nALWAYS"
        # print"\nARGS:%s\nKWARGS:%s" % (args, kwargs)
        pf = kwargs.get('future')
        if pf:
            pf.store.storage['__running__'] = False

    store = background_store()

    f = parallel(async_task, items=[1, 2, 3], concurrency=1, store=store)
    if isinstance(f, FutureParallelTask):
        f.success(success).always(always)

    p = f()
    assert isinstance(p, Process)
    p.join()
    assert p.exitcode == 0

    # Wait for the store to complete
    store.process.join(timeout=30)

    if store.process.is_alive():
        store.process.terminate()

    print "Done"
