from multiprocessing.process import Process

from forq.exceptions import PermanentTaskFailure, IgnoredTaskFailure
from forq.future import future


def async_task(*args, **kwargs):
    print "\nASYNC"
    print"\nARGS:%s\nKWARGS:%s" % (args, kwargs)
    return "DONE"


def async_failed_task(*args, **kwargs):
    print "\nASYNC"
    print"\nARGS:%s\nKWARGS:%s" % (args, kwargs)
    raise PermanentTaskFailure("Test")


def async_ignored_task(*args, **kwargs):
    print "\nASYNC"
    print"\nARGS:%s\nKWARGS:%s" % (args, kwargs)
    raise IgnoredTaskFailure("Test")


def failed(*args, **kwargs):
    print "\nFAILED"
    print"\nARGS:%s\nKWARGS:%s" % (args, kwargs)


def failed_and_ignore(*args, **kwargs):
    print "\nFAILED"
    print"\nARGS:%s\nKWARGS:%s" % (args, kwargs)
    ff = kwargs.get('future')
    ff.error = IgnoredTaskFailure("Test Ignore Failure")


def success(*args, **kwargs):
    print "\nSUCCESS"
    print"\nARGS:%s\nKWARGS:%s" % (args, kwargs)


def always(*args, **kwargs):
    print "\nALWAYS"
    print"\nARGS:%s\nKWARGS:%s" % (args, kwargs)


def test_future_task():

    f = future(async_task)

    f.success(success).always(always)

    p = f()
    assert isinstance(p, Process)
    p.join()
    assert p.exitcode == 0


def test_future_failed_task():

    f = future(async_failed_task)

    f.success(success).always(always).fail(failed)

    p = f()
    assert isinstance(p, Process)
    p.join()
    assert p.exitcode == 1


def test_future_ignored_failed_task():

    f = future(async_ignored_task)

    f.success(success).always(always).fail(failed)

    p = f()
    assert isinstance(p, Process)
    p.join()
    assert p.exitcode == 0


def test_future_failed_task_and_ignore():

    f = future(async_failed_task)

    f.success(success).always(always).fail(failed_and_ignore)

    p = f()
    assert isinstance(p, Process)
    p.join()
    assert p.exitcode == 0

