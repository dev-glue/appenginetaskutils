from taskutils.exceptions import TaskScopeError, TaskNotImplementedError


def run_task(func, args, kwargs, **task_kwargs):
    # logging.info("Run task: %s", log_name)
    # Expected to overload with a method that schedules and task to run
    if task_kwargs.pop("include_scope", None):
        kwargs["scope"] = task_kwargs

    result = func(*args, **kwargs)

    return result


class BaseTaskAdapter(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

        # Get extras (passed to unpickle functions)
        self.scope = self.kwargs.pop("scope", {})
        if not isinstance(self.scope, dict):
            raise TaskScopeError()

        # Update extras with kw args
        if self.kwargs.pop('include_scope', None):
            self.scope['include_scope'] = True

    # noinspection PyUnusedLocal
    def run(self, func, args, kwargs, **run_kwargs):
        raise TaskNotImplementedError()

    @staticmethod
    def encode_function(func, *args, **kwargs):
        raise TaskNotImplementedError()

    @staticmethod
    def callback(data, **kwargs):
        raise TaskNotImplementedError()

    @staticmethod
    def base_route():
        raise TaskNotImplementedError()

    @staticmethod
    def callback_route():
        raise TaskNotImplementedError()


class TaskAdapter(BaseTaskAdapter):
    """
        The simple TaskAdapter doesn't encode functions nor does it route or handle routes it simply runs the task

    """
    def __init__(self, *args, **kwargs):
        super(TaskAdapter, self).__init__(*args, **kwargs)

    # noinspection PyUnusedLocal
    def run(self, func, args, kwargs, **run_kwargs):
        # This method should be overloaded
        return run_task(func, args, kwargs, **self.scope)

