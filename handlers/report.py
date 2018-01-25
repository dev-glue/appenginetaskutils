import json

from flask import request, render_template
from google.appengine.ext import ndb


def get_report(app):
    @app.route('/report')
    def report():
        keystr = request.args.get('key')
        level = int(request.args.get('level', 0))
        if keystr:
            key = ndb.Key(urlsafe=keystr)
            obj = key.get()

            def future_map(future, flevel):
                if future:
                    urlsafe = future.key.urlsafe()
                    return "<a href='report?key=%s&level=%s'>%s</a>" % (urlsafe, flevel, urlsafe)
                else:
                    return None

            obj_json = obj.to_dict(level=level, max_level=level + 5, future_map_fn=future_map)
            return render_template(
                "report.html",
                objjson=json.dumps(obj_json, indent=2, sort_keys=True),
                keystr=keystr
            )
        else:
            return render_template(
                "report.html"
            )

    return report
