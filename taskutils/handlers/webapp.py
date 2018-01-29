from __future__ import absolute_import
import logging
# noinspection PyPackageRequirements
from google.appengine.ext import webapp
from taskutils.exceptions import SingularTaskFailure, PermanentTaskFailure
from taskutils.handlers import run_task, DEFAULT_STF_MESSAGE, DEFAULT_XSRF_MESSAGE
from taskutils.task import default_adapter_factory

# noinspection SpellCheckingInspection
DEFAULT_XSRF_CHECK = None


class TaskHandler(webapp.RequestHandler):
    # noinspection PyUnusedLocal
    def post(self, name):
        # noinspection PyCallingNonCallable
        if not callable(DEFAULT_XSRF_CHECK) or DEFAULT_XSRF_CHECK(self.request.headers):
            try:
                return run_task(self.request.body, headers=self.request.headers, request=self.request)
            except SingularTaskFailure as e:
                msg = "%s%s" % (DEFAULT_STF_MESSAGE, ": %s" % e.message if e.message else "")
                logging.debug(msg)
                self.error(408)
            except PermanentTaskFailure:
                pass  # ignore
        else:
            logging.critical(DEFAULT_XSRF_MESSAGE)
            self.error(403)


def add_route(routes, task_handler=TaskHandler, adapter_factory=None):
    adapter_factory = adapter_factory or default_adapter_factory()

    def _get_routing_url():
        return "%s/(.*)" % adapter_factory.base_route

    routes.append((_get_routing_url(), task_handler))

