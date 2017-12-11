import colander
from cornice import Service
from cornice.resource import resource
from cornice.validators import colander_validator
from pyramid.httpexceptions import HTTPNotFound, HTTPBadRequest, HTTPException

from moisturizer.models import (
    DescriptorModel,
    ModelNotExists,
    UserModel,
    infer_model
)
from moisturizer.exceptions import format_http_error, parse_exception
from moisturizer.utils import (
    merge_dicts,
    build_request,
    build_response,
    follow_subrequest
)


batch = Service(name="batch", path="/batch")
meta_service = Service(name="server_info", path="/__heartbeat__")


@meta_service.get()
def im_alive(request):
    descriptor_key = request.registry.settings['moisturizer.descriptor_key']
    user_key = request.registry.settings['moisturizer.user_key']

    try:
        infer_model(descriptor_key)
        descriptors = True
    except Exception:
        descriptors = False

    try:
        infer_model(user_key)
        users = True
    except Exception:
        users = False

    return {
        "server": True,
        "schema": descriptors,
        "auth": users,
    }


valid_http_method = colander.OneOf(('GET', 'HEAD', 'DELETE', 'TRACE',
                                    'POST', 'PUT', 'PATCH'))


def string_values(node, cstruct):
    """Validate that a ``colander.Mapping`` only has strings in its values.

    .. warning::

        Should be associated to a ``colander.Mapping`` schema node.
    """
    are_strings = [isinstance(v, str) for v in cstruct.values()]
    if not all(are_strings):
        error_msg = '{} contains non string value'.format(cstruct)
        raise colander.Invalid(node, error_msg)


class BatchRequestSchema(colander.MappingSchema):
    method = colander.SchemaNode(colander.String(),
                                 validator=valid_http_method,
                                 missing=colander.drop)
    path = colander.SchemaNode(colander.String(),
                               validator=colander.Regex('^/'))
    headers = colander.SchemaNode(colander.Mapping(unknown='preserve'),
                                  validator=string_values,
                                  missing=colander.drop)
    body = colander.SchemaNode(colander.Mapping(unknown='preserve'),
                               missing=colander.drop)

    @staticmethod
    def schema_type():
        return colander.Mapping(unknown='raise')


class BatchPayloadSchema(colander.MappingSchema):
    defaults = BatchRequestSchema(missing=colander.drop).clone()
    requests = colander.SchemaNode(colander.Sequence(),
                                   BatchRequestSchema())

    @staticmethod
    def schema_type():
        return colander.Mapping(unknown='raise')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # On defaults, path is not mandatory.
        self.get('defaults').get('path').missing = colander.drop

    def deserialize(self, cstruct=colander.null):
        """Preprocess received data to carefully merge defaults."""
        if cstruct is not colander.null:
            defaults = cstruct.get('defaults')
            requests = cstruct.get('requests')
            if isinstance(defaults, dict) and isinstance(requests, list):
                for request in requests:
                    if isinstance(request, dict):
                        merge_dicts(request, defaults)
        return super().deserialize(cstruct)


class BatchRequest(colander.MappingSchema):
    body = BatchPayloadSchema()


@batch.post(schema=BatchRequest(), validators=(colander_validator,))
def batch_me(request):
    payload = request.validated['body']
    requests = payload.get('requests')
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

    return payload


@resource(collection_path='/schemas/{table}/entries',
          path='/schemas/{table}/entries/{id}',)
class InferredObjectResource(object):
    ALLOW_WRITE_TO_SCHEMA = ('POST', 'PUT', 'PATCH',)

    def __init__(self, request, context=None):
        self.request = request
        self.table = self.request.matchdict['table']
        self.id = self.request.matchdict.get('id')
        self.principals = (getattr(self.request, 'principals', None) or
                           self.request.effective_principals)

    @property
    def Model(self):
        try:
            return infer_model(self.table, self.payload)
        except ModelNotExists as e:
            raise format_http_error(HTTPNotFound, e)

    @property
    def payload(self):
        if self.request.method not in self.ALLOW_WRITE_TO_SCHEMA:
            return

        payload = self.request.json or {}
        payload_id = payload.get('id')

        if payload_id and self.id and str(payload_id) != self.id:
            e = Exception()
            raise format_http_error(HTTPBadRequest, e)

        if self.id:
            payload['id'] = self.id

        if self.request.method in ('POST', 'PUT'):
            payload.setdefault('owner', self.request.user.id)

        return payload

    @property
    def query(self):
        return self.Model.all()

    @property
    def entry(self):
        return self.Model.get(id=self.id)

    def collection_post(self):
        new = self.Model.create(**self.payload)
        return new.serialize()

    def collection_get(self):
        return [e.serialize() for e in self.query]

    def collection_delete(self):
        return [self._serialize_and_delete(e) for e in self.query]

    def get(self):
        return self.entry.serialize()

    def delete(self):
        return self._serialize_and_delete(self.entry)

    def put(self):
        Model = self.Model
        try:
            Model.objects.get(id=self.id).delete()
        except Model.DoesNotExist:
            pass

        new = self.Model(**self.payload).save()
        return new.serialize()

    def patch(self):
        self.entry.update(**self.payload)
        return self.entry.serialize()

    def _serialize_and_delete(self, e):
        result = e.serialize()
        e.delete()
        return result


@resource(collection_path='/schemas',
          path='/schemas/{id}',)
class InferredSchemaResource(InferredObjectResource):
    ALLOW_WRITE_TO_SCHEMA = ('POST', 'PUT', 'PATCH', 'DELETE',)

    def __init__(self, request, context=None):
        self.request = request
        self.table = self.request.matchdict.get('id')
        self.principals = self.request.effective_principals

    @property
    def Model(self):
        return DescriptorModel

    def collection_delete(self):
        return [self._serialize_and_delete(e) for e in
                DescriptorModel.objects.all()
                if e.table not in (DescriptorModel.__keyspace__,
                                   UserModel.__keyspace__)]


@resource(collection_path='/users',
          path='/users/{id}',)
class UserResource(InferredObjectResource):

    def __init__(self, request, context=None):
        self.request = request
        self.id = self.request.matchdict.get('id')
        self.principals = self.request.effective_principals

    @property
    def Model(self):
        return UserModel
