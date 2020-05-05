from multiprocessing.process import Process

from forq.task import task


def test_decorated_task():
    @task
    def async_task():
        print "\nASYNC"

    p = async_task()
    assert isinstance(p, Process)
    p.join()
    assert p.exitcode == 0


def test_failing_decorated_task():
    @task
    def async_task():
        raise

    p = async_task()
    assert isinstance(p, Process)
    p.join()
    assert p.exitcode == 1


def test_decorated_task_with_context():
    @task(1, 2, 3, 4, test=1, include_context=True)
    def async_task(*args, **kwargs):
        print "\nASYNC\n%s" % kwargs
        assert kwargs.get("context").get("test") == 1

    p = async_task()
    assert isinstance(p, Process)
    p.join()
    assert p.exitcode == 0


def test_task():
    def async_task(*args, **kwargs):
        print "\nASYNC"

    t = task(async_task)

    p = t()
    assert isinstance(p, Process)
    p.join()
    assert p.exitcode == 0


def test_task_wth_context():
    def async_task(*args, **kwargs):
        print "\nASYNC\n%s" % kwargs
        assert kwargs.get("context").get("test") == 1

    t = task(async_task, 1, 2, 3, 4, test=1, include_context=True)

    p = t()
    assert isinstance(p, Process)
    p.join()
    assert p.exitcode == 0
