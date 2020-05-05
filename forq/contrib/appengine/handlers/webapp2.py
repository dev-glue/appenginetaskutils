from __future__ import absolute_import
import logging
# noinspection PyPackageRequirements
import webapp2
# noinspection PyPackageRequirements
from forq.exceptions import SingularTaskFailure, PermanentTaskFailure, TaskPermissionError
from ..handlers import handle_callback, callback_route


def add_route(app=None):
    app = app or webapp2.get_app()
    callback = handle_callback
    route = callback_route()

    # noinspection PyUnusedLocal
    def task_handler(request, *args, **kwargs):
        # noinspection PyCallingNonCallable
        try:
            return callback(request.body, headers=request.headers)
        except SingularTaskFailure as e:
            logging.debug(e.message)
            webapp2.abort(408)
        except TaskPermissionError as e:
            logging.exception(e.message)
            webapp2.abort(403)
        except PermanentTaskFailure as e:
            logging.exception(e.message)

    app.routes.append((route, task_handler))
