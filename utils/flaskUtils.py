# handy utility methods for Flask
from datetime import timedelta
from flask import request, _request_ctx_stack, make_response, current_app
from optparse import OptionGroup
from functools import update_wrapper

import logging

def makeFlaskOpts(parser):
    flaskOpts = OptionGroup(parser, 'Flask Options')

    flaskOpts.add_option('--public', action='store_true', dest='public', default=False,
        help='By default a flask server is not accessible from any external ip address, if this is set, the Flask web server will accept connections from external addresses, can be dangerous if used in conjunction with the debug mode.')

    return flaskOpts

# Flask doesnt seem to support PUT and DELETE methods, the usual hack for this is for
# clients to instead use a POST and include an "override method" in the http header,
# this method will be run before the url is routed to override the method so
# that in the url bind you can include 'PUT' and 'DELETE' in the methods arg
def httpMethodOverride():
    if 'X-HTTP-Method-Override' in request.headers:
        method = request.headers['X-HTTP-Method-Override'].upper()
        logging.debug('detected a method override, X-HTTP-Method-Override = %s', method)

        request.environ['REQUEST_METHOD'] = method
        ctx = _request_ctx_stack.top
        ctx.url_adapter.default_method = method
    return

# Flask snippet taken from http://flask.pocoo.org/snippets/56/ to allow cross-domain requests

def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator

