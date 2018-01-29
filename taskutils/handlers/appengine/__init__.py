def xsrf_check_taskname(headers):
    """ Check if we are currently running from a task queue """
    # As stated in the doc (https://developers.google.com/appengine/docs/python/taskqueue/overview-push#Task_Request_Headers)
    # These headers are set internally by Google App Engine.
    # If your request handler finds any of these headers, it can trust that the request is a Task Queue request.
    # If any of the above headers are present in an external user request to your App, they are stripped.
    # The exception being requests from logged in administrators of the application, who are allowed to set the headers for testing purposes.
    return headers.get('X-Appengine-TaskName')
