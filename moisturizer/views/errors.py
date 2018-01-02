from pyramid.httpexceptions import (
    HTTPNotFound,
    HTTPUnauthorized,
    HTTPForbidden,
)
from pyramid.view import (
    forbidden_view_config,
    notfound_view_config,
)
from moisturizer.errors import format_http_error


@forbidden_view_config()
def forbidden_view(request):
    if request.authenticated_userid is None:
        return format_http_error(HTTPUnauthorized, HTTPUnauthorized())
    return format_http_error(HTTPForbidden, HTTPForbidden())


@notfound_view_config()
def notfound_view(request):
    return format_http_error(HTTPNotFound, HTTPNotFound())
