import json
from urllib.parse import unquote

from pyramid import httpexceptions
from pyramid.request import Request, apply_request_extensions
from pyramid.view import render_view_to_response


def build_request(original, dict_obj):
    """
    Transform a dict object into a :class:`pyramid.request.Request` object.

    It sets a ``parent`` attribute on the resulting request assigned with
    the `original` request specified.

    :param original: the original request.
    :param dict_obj: a dict object with the sub-request specifications.
    """
    # api_prefix = '/{}'.format(original.upath_info.split('/')[1])
    path = dict_obj['path']
    # if not path.startswith(api_prefix):
    #     path = api_prefix + path

    path = path.encode('utf-8')
    method = dict_obj.get('method', 'GET')

    headers = dict(original.headers)
    headers.update(**dict_obj.get('headers') or {})
    # Body can have different length, do not use original header.
    headers.pop('Content-Length', None)

    payload = dict_obj.get('body') or ''

    # Payload is always a dict (from ``BatchRequestSchema.body``).
    # Send it as JSON for subrequests.
    if isinstance(payload, dict):
        headers['Content-Type'] = 'application/json'
        dump = json.dumps(payload)

    request = Request.blank(path=path.decode('latin-1'),
                            headers=headers,
                            POST=dump,
                            method=method,
                            json=payload)

    request.registry = original.registry
    apply_request_extensions(request)

    # This is used to distinguish subrequests from direct incoming requests.
    # See :func:`kinto.core.initialization.setup_logging()`
    request.parent = original
    request.principals = original.principals
    request.user = original.user

    return request


def build_response(response, request):
    """
    Transform a :class:`pyramid.response.Response` object into a serializable
    dict.

    :param response: a response object, returned by Pyramid.
    :param request: the request that was used to get the response.
    """
    dict_obj = {}
    dict_obj['path'] = unquote(request.path)
    dict_obj['status'] = response.status_code
    dict_obj['headers'] = dict(response.headers)

    body = ''
    if request.method != 'HEAD':
        # XXX : Pyramid should not have built response body for HEAD!
        try:
            body = response.json
        except ValueError:
            body = response.body
    dict_obj['body'] = body

    return dict_obj


PRIMITIVES = [int, bool, float, str, dict, list, type(None)]


def cast_primitive(value):
    if any(isinstance(value, typ) for typ in PRIMITIVES):
        return value
    return str(value)


def follow_subrequest(request, subrequest, **kwargs):
    """Run a subrequest (e.g. batch), and follow the redirection if any.

    :rtype: tuple
    :returns: the reponse and the redirection request (or `subrequest`
              if no redirection happened.)
    """
    try:
        try:
            return request.invoke_subrequest(subrequest, **kwargs), subrequest
        except Exception as e:
            resp = render_view_to_response(e, subrequest)
            if not resp or resp.status_code >= 500:
                raise e
            raise resp
    except httpexceptions.HTTPRedirection as e:
        new_location = e.headers['Location']
        new_request = Request.blank(path=new_location,
                                    headers=subrequest.headers,
                                    POST=subrequest.body,
                                    method=subrequest.method)
        new_request.bound_data = subrequest.bound_data
        new_request.parent = getattr(subrequest, 'parent', None)
        return request.invoke_subrequest(new_request, **kwargs), new_request


def merge_dicts(a, b):
    """Merge b into a recursively, without overwriting values.

    :param dict a: the dict that will be altered with values of `b`.
    """
    for k, v in b.items():
        if isinstance(v, dict):
            merge_dicts(a.setdefault(k, {}), v)
        else:
            a.setdefault(k, v)
