import datetime

from cornice import Service
from cornice.resource import resource
from cornice.validators import colander_validator, colander_body_validator
from pyramid.decorator import reify
from pyramid.httpexceptions import (
    HTTPNotFound,
    HTTPBadRequest,
    HTTPException,
    HTTPUnauthorized,
    HTTPForbidden,
)
from pyramid.security import ALL_PERMISSIONS, Allow, Authenticated
from pyramid.view import forbidden_view_config

from moisturizer.models import (
    DescriptorModel,
    PermissionModel,
    UserModel,
)
from moisturizer.exceptions import format_http_error, parse_exception
from moisturizer.schemas import (
    InferredTypeSchema,
    InferredObjectSchema,
    UserSchema,
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
    # descriptor_key = request.registry.settings['moisturizer.descriptor_key']
    # user_key = request.registry.settings['moisturizer.user_key']

    try:
        descriptors = True
    except Exception:
        descriptors = False

    try:
        users = True
    except Exception:
        users = False

    return {
        "server": True,
        "schema": descriptors,
        "users": users,
    }


@forbidden_view_config()
def forbidden_view(request):
    if request.authenticated_userid is None:
        raise format_http_error(HTTPUnauthorized, HTTPUnauthorized())
    raise format_http_error(HTTPForbidden, HTTPUnauthorized())


@resource(collection_path='/types/{type_id}/objects',
          path='/types/{type_id}/objects/{id}',
          validators=('deserialize_model',),
          permission="authenticated",)
class ObjectResource(object):
    ALLOW_WRITE_TO_SCHEMA = ('POST', 'PUT', 'PATCH',)

    def __init__(self, request, context=None):
        self.request = request
        self.principals = (getattr(self.request, 'principals', None) or
                           self.request.effective_principals)

    # Factory methods

    def deserialize_model(self, request, **kwargs):
        kwargs['schema'] = self.schema
        return colander_body_validator(request, **kwargs)

    @reify
    def schema(self):
        """Returns the schema for the current type."""
        return InferredObjectSchema().bind(descriptor=self.descriptor)

    def __acl__(self):
        """ACL permissions for the current resource."""
        return (
            (Allow, Authenticated, ALL_PERMISSIONS),
        )

    # Subclass properties

    @reify
    def type_id(self):
        return self.request.matchdict.get('type_id')

    @reify
    def id(self):
        return self.request.matchdict.get('id')

    @reify
    def descriptor(self):
        """
        (moisturizer.models.DescriptorModel): Type descriptor of the
        current table. May create a new descriptor if not existing and
        schema changes are allowed.
        """

        # First, we attempt to find a descriptor for the current model.
        try:
            return DescriptorModel.get(id=self.type_id)

        # If not exists, check if we can create it.
        except DescriptorModel.DoesNotExist as e:
            if self.request.method not in self.ALLOW_WRITE_TO_SCHEMA:
                raise format_http_error(HTTPForbidden, e)

        return DescriptorModel.create(id=self.type_id)

    @reify
    def Model(self):
        """
        (moisturizer.models.InferredModel): Model matching the given
        type decriptor. May be used as storage abstraction.
        """
        self.preprocess()
        return self.descriptor.model

    @reify
    def query(self):
        return self.Model.all()

    @reify
    def entry(self):
        try:
            return self.Model.get(id=self.id)
        except self.Model.DoesNotExist as e:
            e.type_id = self.type_id
            raise format_http_error(HTTPNotFound(), e)

    @reify
    def payload(self):
        return self.request.validated

    def preprocess(self):
        # FIXME: move to schema.
        payload = self.schema.flatten(self.request.validated) or {}
        if self.id is not None:
            payload.setdefault('id', self.id)
        self.descriptor.infer_schema_change(payload)
        self.request.validated = payload

    def permissions(self):
        # Admins can do wathever they want.
        if self.request.user.role == UserModel.ROLE_ADMIN:
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
            new = self.Model.create(**self.payload)
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
        obj = self.Model(**self.request.validated)
        obj.save()
        return self.postprocess(obj)

    def patch(self):
        self.entry.update(**self.payload)
        return self.postprocess(self.entry)

    def _serialize_and_delete(self, e):
        e.delete()
        return e

    def postprocess(self, result):
        if isinstance(result, list):
            return [self.schema.serialize(self.schema.unflatten(e))
                    for e in result]
        return self.schema.serialize(self.schema.unflatten(result))


@resource(collection_path='/types',
          path='/types/{id}',
          validators=('deserialize_model',),
          permission="authenticated",)
class InferredTypeResource(ObjectResource):
    ALLOW_WRITE_TO_SCHEMA = ('POST', 'PUT', 'PATCH', 'DELETE',)

    @property
    def schema(self):
        return InferredTypeSchema().bind(type_id=self.descriptor)

    @property
    def type_id(self):
        return 'descriptor_model'

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
          validators=('deserialize_model',),
          permission="authenticated",)
class UserResource(ObjectResource):
    @reify
    def schema(self):
        return UserSchema().bind(descriptor=self.descriptor)

    @reify
    def Model(self):
        return UserModel

    @reify
    def type_id(self):
        return 'user_model'


@resource(collection_path='/types/{type_id}/permissions',
          path='/types/{type_id}/permissions/{id}',
          validators=('deserialize_model',),
          permission="authenticated",)
class TypePermissionResource(ObjectResource):
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
          path='/types/{type_id}/objects/{id}/permissions/{owner_id}',
          validators=('deserialize_model',),
          permission="authenticated",)
class ObjectPermissionResource(ObjectResource):
    @reify
    def Model(self):
        return PermissionModel

    def deserialize_model(self, request, **kwargs):
        kwargs['schema'] = self.schema
        return colander_body_validator(request, **kwargs)

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
