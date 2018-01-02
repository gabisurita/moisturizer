
from cornice import Service
from cornice.validators import colander_validator
from pyramid.httpexceptions import HTTPException

from moisturizer.errors import parse_exception
from moisturizer.schemas import BatchRequest
from moisturizer.utils import (
    build_request,
    build_response,
    follow_subrequest
)


batch = Service(name="batch", path="/batch")


@batch.post(schema=BatchRequest(), validators=(colander_validator,))
def batch_me(request):
    argument = request.validated['body']
    requests = argument.get('requests')
    responses = []

    request.principals = request.effective_principals

    for subrequest_spec in requests:
        subrequest = build_request(request, subrequest_spec)
        try:
            # Invoke subrequest without individual transaction.
            resp, subrequest = follow_subrequest(request,
                                                 subrequest,
                                                 use_tweens=False)
        except HTTPException as e:
            if e.content_type == 'application/json':
                resp = e
            else:
                resp = parse_exception(e)

        dict_resp = build_response(resp, subrequest)
        responses.append(dict_resp)

    return {
        'responses': responses
    }

    return argument
