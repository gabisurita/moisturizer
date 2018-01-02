import datetime

from cornice.resource import resource
from cornice.validators import colander_body_validator
from pyramid.decorator import reify
from pyramid.httpexceptions import (
    HTTPNotFound,
    HTTPForbidden,
)
from pyramid.security import ALL_PERMISSIONS, Allow, Authenticated
from moisturizer.models import (
    DescriptorModel,
    PermissionModel,
    UserModel,
    DoesNotExist,
)
from moisturizer.errors import format_http_error
from moisturizer.schemas import (
    InferredTypeSchema,
    InferredObjectSchema,
    UserSchema,
    PermissionSchema,
)


class BaseResource(object):
    """
    Base resource to expose moisturizer models.
    """

    model = None
    """
    (moisturizer.models.Model): Model type."""

    schema = None
    """Colander schema for validation and (de)seriliazation."""

    argument = {}
    """Input argument matching the model input interface."""

    filters = {}
    """Input filters matching the model filter interface."""

    id = None
    """Resource object id."""

    @reify
    def query(self):
        """Model query resolver."""
        return self.model.filter(**self.filters)

    @reify
    def entry(self):
        """Model entry resolver."""
        try:
            return self.model.get(id=self.id)
        except self.model.DoesNotExist as e:
            raise format_http_error(HTTPNotFound, e)

    def __init__(self, request, context=None):
        self.request = request
        self.context = context

    def __acl__(self):
        return (
            (Allow, Authenticated, ALL_PERMISSIONS),
        )

    def deserialize_model(self, request, **kwargs):
        kwargs['schema'] = self.schema
        return colander_body_validator(request, **kwargs)

    # endpoints

    def collection_post(self):
        new = self.model.create(**self.argument)
        return self._postprocess(new)

    def collection_get(self):
        return self._postprocess(list(self.query))

    def collection_delete(self):
        [e.delete() for e in self.query]
        return self._postprocess(list(self.query))

    def get(self):
        return self._postprocess(self.entry)

    def delete(self):
        self.entry.delete()
        return self._postprocess(self.entry)

    def put(self):
        obj = self.model(**self.argument)
        obj.save()
        return self._postprocess(obj)

    def patch(self):
        self.argument.update(last_modified=datetime.datetime.now())
        self.entry.update(**self.argument)
        return self._postprocess(self.entry)

    def _postprocess(self, result):
        if isinstance(result, list):
            return [self.schema.serialize(self.schema.unflatten(e))
                    for e in result]
        return self.schema.serialize(self.schema.unflatten(result))


class DescribedResource(BaseResource):
    """
    Resource for exposing moisturizer models that are given by a
    descriptor instance instead of explicitly declared.
    """

    ALLOW_WRITE_TO_SCHEMA = ('POST', 'PUT', 'PATCH',)

    type_id = None
    """Id of the described type."""

    @reify
    def model(self):
        if self.argument:
            self.descriptor.infer_schema_change(self.argument)
        return self.descriptor.model

    @reify
    def schema(self):
        return InferredObjectSchema().bind(descriptor=self.descriptor)

    @reify
    def argument(self):
        argument = self.schema.flatten(self.request.validated) or {}
        if self.id is not None:
            argument.setdefault('id', self.id)
        return argument

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


class SecureDescribedResource(DescribedResource):

    @reify
    def permissions(self):
        user = self.request.user

        # Admins can do wathever they want.
        if user.role == UserModel.ROLE_ADMIN:
            return user.permissions.admin()

        try:
            return user.permissions.get(id=self.type_id or self.id)
        except DoesNotExist:
            return PermissionModel()

    @reify
    def filters(self):
        filters = super().filters
        if self.permissions.read or self.permissions.write:
            return filters

        raise HTTPForbidden()

    @reify
    def model(self):
        def protect_model_method(method, permission):
            def inner(*args, **kwargs):
                if not permission:
                    raise HTTPForbidden()
                return method(*args, **kwargs)
            return inner

        model = super().model
        model.save = protect_model_method(model.save,
                                          self.permissions.write)
        model.update = protect_model_method(model.update,
                                            self.permissions.write)
        model.create = protect_model_method(model.create,
                                            self.permissions.create)

        return model


@resource(collection_path='/types/{type_id}/objects',
          path='/types/{type_id}/objects/{id}',
          validators=('deserialize_model',),
          permission="authenticated",)
class ObjectResource(SecureDescribedResource):
    @reify
    def id(self):
        return self.request.matchdict.get('id')

    @reify
    def type_id(self):
        return self.request.matchdict.get('type_id')


@resource(collection_path='/types',
          path='/types/{id}',
          validators=('deserialize_model',),
          permission="authenticated",)
class InferredTypeResource(DescribedResource):
    ALLOW_WRITE_TO_SCHEMA = ('POST', 'PUT', 'PATCH', 'DELETE',)

    @reify
    def id(self):
        return self.request.matchdict.get('id')

    @reify
    def type_id(self):
        return 'descriptor_model'

    @reify
    def schema(self):
        return InferredTypeSchema().bind(type_id=self.descriptor)

    def collection_delete(self):
        # TODO: Implement excludes as models interface.
        preserve = ['user_model', 'permission_model', 'descriptor_model']
        self.query = [e for e in self.query if e.id not in preserve]
        return super().collection_delete()


@resource(collection_path='/users',
          path='/users/{id}',
          validators=('deserialize_model',),
          permission="authenticated",)
class UserResource(SecureDescribedResource):
    @reify
    def id(self):
        return self.request.matchdict.get('id')

    @reify
    def type_id(self):
        return 'user_model'

    @reify
    def schema(self):
        return UserSchema().bind(descriptor=self.descriptor)

    @reify
    def model(self):
        return UserModel


@resource(path='/users/{user_id}/permissions/{type_id}',
          collection_path='/users/{user_id}/permissions',
          validators=('deserialize_model',),
          permission="authenticated",)
class TypePermissionResource(SecureDescribedResource):
    ALLOW_WRITE_TO_SCHEMA = ('POST', 'PUT', 'PATCH', 'DELETE',)

    @reify
    def id(self):
        return self.request.matchdict.get('type_id')

    @reify
    def user_id(self):
        return self.request.matchdict.get('user_id')

    @reify
    def schema(self):
        return PermissionSchema()

    @reify
    def model(self):
        try:
            return UserModel.get(id=self.user_id).permissions
        except DoesNotExist as e:
            raise format_http_error(HTTPNotFound, e)
