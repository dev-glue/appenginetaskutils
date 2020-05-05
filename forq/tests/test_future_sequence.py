import datetime
from multiprocessing.process import Process

from forq.future.utils.sequence import sequence


def test_future_sequence():

    def async_task(item, index, items, *args, **kwargs):
        print "\nASYNC"
        print"\nARGS:%s\nKWARGS:%s" % (args, kwargs)
        print item, index, items, datetime.datetime.utcnow()
        return item * index

    def success(*args, **kwargs):
        print "\nSUCCESS"
        print"\nARGS:%s\nKWARGS:%s" % (args, kwargs)
        result = kwargs.get('result')
        print list(result)

    def always(*args, **kwargs):
        print "\nALWAYS"
        print"\nARGS:%s\nKWARGS:%s" % (args, kwargs)

    f = sequence(async_task, items=[1, 2, 3])

    f.success(success).always(always)

    p = f()
    assert isinstance(p, Process)
    p.join()
    assert p.exitcode == 0

