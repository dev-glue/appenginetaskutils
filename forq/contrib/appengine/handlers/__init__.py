from forq.exceptions import PermanentTaskFailure
from forq.utils import decode_function, encode_function

DEFAULT_CALLBACK_ROUTE = "%s%s"
DEFAULT_CALLBACK_ROUTE_BASE = "/_ah/task"
DEFAULT_CALLBACK_MATCH = "/(.*)"


def callback_route():
    return DEFAULT_CALLBACK_ROUTE % (DEFAULT_CALLBACK_ROUTE_BASE, DEFAULT_CALLBACK_MATCH)


def generate_callback_route(path):
    return DEFAULT_CALLBACK_ROUTE % (DEFAULT_CALLBACK_ROUTE_BASE, path)


def callback_encode(func, *args, **kwargs):
    return encode_function(func, *args, **kwargs)


def handle_callback(data, **context):

    # Schedule
    try:
        func, args, kwargs = decode_function(data)
    except Exception as err:
        raise PermanentTaskFailure(err)

    context.update(kwargs)
    return func(*args, **context)
