from cornice import Service
from cornice.resource import resource
from pyramid.httpexceptions import HTTPNotFound, HTTPBadRequest

from moisturizer.models import (
    DescriptorModel,
    ModelNotExists,
    UserModel,
    infer_model
)
from moisturizer.exceptions import format_http_error


meta_service = Service(name="server_info", path="/__heartbeat__")


@meta_service.get()
def im_alive(request):
    descriptor_key = request.registry.settings['moisturizer.descriptor_key']
    try:
        infer_model(descriptor_key)
        descriptors = True
    except Exception:
        descriptors = False

    return {
        "self": True,
        "descriptors": descriptors,
    }


@resource(collection_path='/schemas/{table}/entries',
          path='/schemas/{table}/entries/{id}',)
class InferredObjectResource(object):
    ALLOW_WRITE_TO_SCHEMA = ('POST', 'PUT', 'PATCH',)

    def __init__(self, request, context=None):
        self.request = request
        self.table = self.request.matchdict['table']
        self.id = self.request.matchdict.get('id')
        self.principals = self.request.effective_principals

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

        return payload

    @property
    def query(self):
        return self.Model.all()

    @property
    def entry(self):
        return self.Model.get(id=self.id)

    def collection_post(self):
        new = self.Model.create(**self.request.json)
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
                if e.table != DescriptorModel.__keyspace__]


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
