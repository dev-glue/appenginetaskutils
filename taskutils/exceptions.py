class PermanentTaskFailure(Exception):
    pass


class SingularTaskFailure(Exception):
    pass


class TaskPermissionError(Exception):
    pass


class TaskScopeError(Exception):
    pass


class TaskPropertyError(Exception):
    pass


class TaskHandlerError(Exception):
    pass


class TaskNotImplementedError(Exception):
    pass
