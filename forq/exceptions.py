class PermanentTaskFailure(Exception):
    pass


class SingularTaskFailure(Exception):
    pass


class IgnoredTaskFailure(Exception):
    pass


class MissingTaskFunction(Exception):
    pass


class TaskPermissionError(Exception):
    pass


class TaskContextError(Exception):
    pass


class TaskPropertyError(Exception):
    pass


class TaskHandlerError(Exception):
    pass


class TaskNotImplementedError(Exception):
    pass


class TaskTombstonedError(Exception):
    pass


class TaskAlreadyExistsError(Exception):
    pass

