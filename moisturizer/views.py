import datetime

from cornice import Service
from cornice.resource import resource
from cornice.validators import colander_validator, colander_body_validator
from pyramid.decorator import reify
from pyramid.httpexceptions import HTTPNotFound, HTTPBadRequest, HTTPException

from moisturizer.models import (
    DescriptorModel,
    ModelNotExists,
    UserModel,
    PermissionModel,
    infer_model
)
from moisturizer.exceptions import format_http_error, parse_exception
from moisturizer.schemas import (
    InferredTypeSchema,
    InferredObjectSchema,
    BatchRequest
)
from moisturizer.utils import (
    build_request,
    build_response,
    follow_subrequest
)


batch = Service(name="batch", path="/batch")
heartbeat = Service(name="server_info", path="/__heartbeat__")


@heartbeat.get()
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
        "users": users,
    }


@resource(collection_path='/types/{type_id}/objects',
          path='/types/{type_id}/objects/{id}',
          validators=('deserialize_model',))
class InferredObjectResource(object):
    ALLOW_WRITE_TO_SCHEMA = ('POST', 'PUT', 'PATCH',)

    def __init__(self, request, context=None):
        self.request = request
        self.type_id = self.request.matchdict.get('type_id')
        self.id = self.request.matchdict.get('id')
        self.principals = (getattr(self.request, 'principals', None) or
                           self.request.effective_principals)
        self.user = self.request.user

    def deserialize_model(self, request, **kwargs):
        kwargs['schema'] = self.schema
        return colander_body_validator(request, **kwargs)

    @reify
    def schema(self):
        return InferredObjectSchema().bind(type_id=self.type_id)

    @reify
    def Model(self):
        """The Model representing the current resource. The model is inferred
        at runtime by the given ``type_id``."""
        try:
            return infer_model(self.type_id, self.payload)
        except ModelNotExists as e:
            raise format_http_error(HTTPNotFound, e)

    @reify
    def query(self):
        return self.Model.all()

    @reify
    def entry(self):
        return self.Model.get(id=self.id)

    @reify
    def payload(self):
        if self.request.method not in self.ALLOW_WRITE_TO_SCHEMA:
            return

        payload = self.request.validated or {}

        payload.setdefault('last_modified', datetime.datetime.now())
        if self.id is not None:
            payload.setdefault('id', self.id)

        return payload

    @reify
    def permissions(self):
        # Admins can do wathever they want.
        if self.user.role == UserModel.ROLE_ADMIN:
            return PermissionModel.admin

        try:
            # Try to fetch explicit permissions at the backend.
            perms = PermissionModel.get(
                id=self.id if self.id else self.type_id,
                type_id=self.type_id if self.id else '',
                owner=self.request.user.id,
            )

        except PermissionModel.DoesNotExist:
            return PermissionModel()

        return perms

    def collection_post(self):
        try:
            new = self.Model.create(**self.request.validated)
        except Exception as e:
            raise format_http_error(HTTPBadRequest, e)

        return self.postprocess(new)

    def collection_get(self):
        result = [e for e in self.query]
        return self.postprocess(result)

    def collection_delete(self):
        return self.postprocess(
            [self._serialize_and_delete(e) for e in self.query]
        )

    def get(self):
        return self.postprocess(self.entry)

    def delete(self):
        return self.postprocess(
            self._serialize_and_delete(self.entry)
        )

    def put(self):
        Model = self.Model
        try:
            Model.objects.get(id=self.id).delete()
        except Model.DoesNotExist:
            pass

        new = self.Model(**self.request.validated).save()
        return self.postprocess(new)

    def patch(self):
        self.entry.update(**self.payload)
        return self.postprocess(self.entry)

    def _serialize_and_delete(self, e):
        e.delete()
        return e

    def postprocess(self, result):
        if isinstance(result, list):
            return [self.schema.serialize(e) for e in result]
        return self.schema.serialize(result)


@resource(collection_path='/types',
          path='/types/{id}',
          validators=('deserialize_model',))
class InferredTypeResource(InferredObjectResource):
    ALLOW_WRITE_TO_SCHEMA = ('POST', 'PUT', 'PATCH', 'DELETE',)

    @reify
    def schema(self):
        return InferredTypeSchema().bind(type_id=self.type_id)

    @reify
    def Model(self):
        return DescriptorModel

    def deserialize_model(self, request, **kwargs):
        kwargs['schema'] = InferredTypeSchema().bind()
        return colander_body_validator(request, **kwargs)

    @reify
    def type_id(self):
        return self.Model.__keyspace__

    def collection_delete(self):
        return self.postprocess([
            self._serialize_and_delete(e)
            for e in DescriptorModel.objects.all()
            if e.id not in (DescriptorModel.__keyspace__,
                            UserModel.__keyspace__,
                            PermissionModel.__keyspace__)
        ])


@resource(collection_path='/users',
          path='/users/{id}',
          validators=('deserialize_model',))
class UserResource(InferredObjectResource):
    @reify
    def Model(self):
        return UserModel

    @reify
    def type_id(self):
        return self.Model.__keyspace__


@resource(collection_path='/types/{type_id}/permissions',
          path='/types/{type_id}/permissions/{id}',)
class TypePermissionResource(InferredObjectResource):
    @reify
    def Model(self):
        return PermissionModel

    @reify
    def query(self):
        return self.Model.filter(
            id=self.type_id,
            type_id='',
            owner__in=[u.id for u in UserModel.all()],
        )

    @reify
    def entry(self):
        return self.Model.get(
            id=self.type_id,
            type_id='',
            owner=self.id,
        )

    @reify
    def owner_id(self):
        return self.request.matchdict.get('owner_id')


@resource(collection_path='/types/{type_id}/objects/{id}/permissions',
          path='/types/{type_id}/objects/{id}/permissions/{owner_id}',)
class ObjectPermissionResource(InferredObjectResource):

    @reify
    def Model(self):
        return PermissionModel

    @reify
    def query(self):
        return self.Model.filter(
            id=self.id,
            type_id=self.type_id,
            owner__in=[u.id for u in UserModel.all()],
        )

    @reify
    def entry(self):
        return self.Model.get(
            id=self.id,
            type_id=self.type_id,
            owner=self.owner_id,
        )

    @reify
    def owner_id(self):
        return self.request.matchdict.get('owner_id')


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
