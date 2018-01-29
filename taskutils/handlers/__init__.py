from taskutils.exceptions import PermanentTaskFailure, SingularTaskFailure
from taskutils.utils import decode_function

# noinspection SpellCheckingInspection
DEFAULT_XSRF_MESSAGE = 'Detected an attempted XSRF attack: we are not executing from a task queue.'
DEFAULT_STF_MESSAGE = 'Failure executing task, task retry forced.'


def run_task(data, headers=None, request=None):

    """Unpickles and executes a task.

    Args:
      data: An encoded task data
      headers: A dict of request headers (optional)
      request: The original request
    Returns:
      The return value of the function invocation.

    """
    try:
        func, args, kwargs = decode_function(data)
    except Exception as err:
        raise PermanentTaskFailure(err)
    else:
        try:
            func(*args, headers=headers, request=request, **kwargs)
        except SingularTaskFailure:
            raise
        except PermanentTaskFailure:
            raise
        except Exception as err:
            raise PermanentTaskFailure(err)
