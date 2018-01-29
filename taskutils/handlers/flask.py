from __future__ import absolute_import
import flask
import logging
from taskutils.exceptions import SingularTaskFailure, PermanentTaskFailure
from taskutils.handlers import run_task, DEFAULT_STF_MESSAGE, DEFAULT_XSRF_MESSAGE
from taskutils.task import default_adapter_factory

# noinspection SpellCheckingInspection
DEFAULT_XSRF_CHECK = None


def add_route(app, adapter_factory=None):

    # Get adapter (one already created with tasks args) or use adapter_factory or the default adapter factory
    adapter_factory = adapter_factory or default_adapter_factory()

    def _get_routing_url():
        return "%s/(.*)" % adapter_factory.base_route

    # noinspection PyUnusedLocal
    @app.route(_get_routing_url(), methods=["POST"])
    def task(name):
        # noinspection PyCallingNonCallable
        if not callable(DEFAULT_XSRF_CHECK) or DEFAULT_XSRF_CHECK(flask.request.headers):
            try:
                return run_task(flask.request.data, headers=flask.request.headers, request=flask.request)
            except SingularTaskFailure as e:
                msg = "%s%s" % (DEFAULT_STF_MESSAGE, ": %s" % e.message if e.message else "")
                logging.debug(msg)
                return msg, 408
            except PermanentTaskFailure:
                pass  # ignore
        else:
            logging.critical(DEFAULT_XSRF_MESSAGE)
            return DEFAULT_XSRF_MESSAGE, 403



