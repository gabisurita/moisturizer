from cornice import Service
from cornice.resource import resource
from pyramid.httpexceptions import HTTPNotFound, HTTPBadRequest
from moisturizer.models import DescriptorModel, ModelNotExists, infer_model


meta_service = Service(name="server_info", path="/__heartbeat__")


@meta_service.get()
def im_alive(request):
    return {
        "self": "Ok",
        "cassandra": "Ok" if infer_model('table_descriptor') else "Fail",
    }


def parse_exception(exception):
    return {
        'message': str(exception),
        'table': exception.table,
        'type': exception.__class__.__name__,
    }


def format_http_error(http_error, exception):
    return http_error(json={
        'error': parse_exception(exception)
    })


@resource(collection_path='/', path='/{table}')
class InferredSchemaResource(object):
    ALLOW_WRITE_TO_SCHEMA = ('POST', 'PUT', 'PATCH', 'DELETE',)

    def __init__(self, request, context=None):
        self.request = request
        self.table = self.request.matchdict.get('table')

    def collection_get(self):
        return [e.serialize() for e in DescriptorModel.objects.all()]

    def collection_delete(self):
        return [self._serialize_and_delete(e) for e in
                DescriptorModel.objects.all()
                if e.table != DescriptorModel.__keyspace__]

    def get(self):
        DescriptorModel.objects.get(table=self.table).serialize()

    def delete(self):
        descriptor = DescriptorModel.objects.get(table=self.table)
        result = descriptor.serialize()
        descriptor.delete()
        return result

    def _serialize_and_delete(self, e):
        result = e.serialize()
        e.delete()
        return result


@resource(collection_path='/{table}/objects', path='/{table}/objects/{id}')
class InferredObjectResource(object):
    ALLOW_WRITE_TO_SCHEMA = ('POST', 'PUT', 'PATCH',)

    def __init__(self, request, context=None):
        self.request = request
        self.table = self.request.matchdict['table']
        self.id = self.request.matchdict.get('id')

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
    def obj(self):
        return self.Model.objects.get(id=self.id)

    def collection_post(self):
        new = self.Model(**self.request.json).save()
        return new.serialize()

    def collection_get(self):
        return [e.serialize() for e in self.Model.objects.all()]

    def collection_delete(self):
        return [self._serialize_and_delete(e)
                for e in self.Model.objects.all()]

    def get(self):
        return self.Model.objects.get(id=self.id).serialize()

    def delete(self):
        return self._serialize_and_delete(
                self.Model.objects.get(id=self.id).serialize())

    def put(self):
        Model = self.Model
        try:
            Model.objects.get(id=self.id).delete()
        except Model.DoesNotExist:
            pass

        new = self.Model(**self.payload).save()
        return new.serialize()

    def patch(self):
        self.Model.objects.filter(id=self.id).update(**self.payload)
        return self.Model.get(id=self.id).serialize()

    def _serialize_and_delete(self, e):
        result = e.serialize()
        e.delete()
        return result
