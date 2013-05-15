# handy utility methods for Flask
from flask import request, _request_ctx_stack
from optparse import OptionGroup

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
