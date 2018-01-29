from taskutils.task import encode_task


class TaskAdapter(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def run_task(self, encoded_function, log_name):
        pass

