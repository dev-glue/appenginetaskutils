from __future__ import absolute_import
import logging
# noinspection PyPackageRequirements
import webapp2
# noinspection PyPackageRequirements
from taskutils.exceptions import SingularTaskFailure, PermanentTaskFailure, TaskPermissionError, TaskHandlerError
from taskutils.task import default_adapter_factory


def add_route(adapter_factory=None, adapter=None):
    app = webapp2.get_app()
    # Get adapter (one already created with tasks args) or use adapter_factory or the default adapter factory
    adapter_factory = adapter or adapter_factory or default_adapter_factory()

    callback_route = adapter_factory.callback_route()
    callback = adapter_factory.callback

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

    app.routes.append((callback_route, task_handler))
