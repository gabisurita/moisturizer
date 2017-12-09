from cornice import Service
from cornice.resource import resource

from moisturizer.models import infer_model


@resource(collection_path='/{table}', path='/{table}/{id}')
class InferredObjectResource(object):

    ALLOW_WRITE_TO_SCHEMA = ('POST', 'PUT', 'PATCH')

    def __init__(self, request, context=None):
        self.request = request
        self.table = self.request.matchdict['table']
        self.id = self.request.matchdict.get('id')

    @property
    def Model(self):
        payload = (self.request.json if self.request.method in
                   self.ALLOW_WRITE_TO_SCHEMA else None)

        return infer_model(self.table, payload)

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
        self.Model.objects.get(id=self.id).delete()
        new = self.Model(id=self.id, **self.request.json).save()
        return new.serialize()

    def patch(self):
        self.Model.objects.filter(id=self.id).update(**self.request.json)
        return self.Model.get(id=self.id).serialize()

    def _serialize_and_delete(e):
        result = e.serialize()
        e.delete()
        return result


meta_service = Service(name="server_info", path="/")


@meta_service.get()
def im_alive(request):
    return {
        "self": "Ok",
        "cassandra": "Ok" if infer_model('table_descriptor') else "Fail",
    }
