from __future__ import absolute_import
import flask
from forq.exceptions import SingularTaskFailure, PermanentTaskFailure, TaskPermissionError
from ..handlers import handle_callback, callback_route


def add_route(app):
    callback = handle_callback
    route = callback_route()

    # noinspection PyUnusedLocal
    @app.route(route, methods=["POST"])
    def task(*args, **kwargs):
        request = flask.request
        # noinspection PyCallingNonCallable
        try:
            return callback(request.data, headers=request.headers)
        except SingularTaskFailure as e:
            return e.message, 408
        except PermanentTaskFailure:
            pass  # ignore
        except TaskPermissionError as e:
            return e.message, 403
