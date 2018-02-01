from __future__ import absolute_import
import flask
from taskutils.exceptions import SingularTaskFailure, PermanentTaskFailure, TaskPermissionError, TaskHandlerError
from taskutils.task import default_adapter_factory


def add_route(app, adapter_factory=None, adapter=None):
    # Get adapter (one already created with tasks args) or use adapter_factory or the default adapter factory
    adapter_factory = adapter or adapter_factory or default_adapter_factory()

    if not callable(getattr(adapter_factory, 'callback_route')) or not callable(getattr(adapter_factory, 'callback')):
        raise TaskHandlerError('Adapter must have a callback_route and callback methods')

    routing_url = adapter_factory.routing_url()
    callback = adapter_factory.callback

    # noinspection PyUnusedLocal
    @app.route(routing_url, methods=["POST"])
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
