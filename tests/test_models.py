import pytest
from cassandra.cqlengine import management

from moisturizer import (
    main
)
from moisturizer.models import (
    DescriptorModel,
    UserModel,
    PermissionModel,
    infer_model,
)


@pytest.fixture(autouse=True, scope="module")
def setup_migrations():
    yield main({
        'moisturizer.keyspace': 'test',
    })
    management.drop_keyspace('test')


@pytest.fixture(autouse=True)
def clean_infer_models():
    yield None
    models = DescriptorModel.all()
    ignores = map(lambda m: m.__keyspace__,
                  (DescriptorModel, UserModel, PermissionModel))

    for model in models:
        if model.id not in ignores:
            model.delete()


@pytest.mark.parametrize("type_id, payload", [
    ('hello', {}),
    ('hello', {'field': 'foo'}),
    ('hello', {'field': ''}),
    ('hello', {'field': 0}),
    ('hello', {'field': 42}),
    ('hello', {'field': 42.42}),
    # ('hello', {'field': True}),
    # ('hello', {'field': False}),
])
class TestModelInference(object):

    def test_type_inference(self, type_id, payload):
        Model = infer_model(type_id, payload=payload)
        created = Model.create(**payload)
        assert getattr(created, 'field', None) == payload.get('field')

    def test_model_mutation(self, type_id, payload):
        # Infer once using payload
        Model = infer_model(type_id, payload=payload)

        # Mutate payload
        new_payload = payload.copy()
        new_payload['field2'] = 'bar'
        Model = infer_model(type_id, new_payload)
        created = Model.create(**new_payload)

        assert getattr(created, 'field', None) == new_payload.get('field')
        assert getattr(created, 'field2', None) == new_payload.get('field2')

    def test_invalid_model_mutation(self, type_id, payload):
        field = payload.get('field')

        # FIXME: Bool doesn't seem quite right here
        if field is None or isinstance(field, str) or isinstance(field, bool):
            return

        # Infer once using payload
        Model = infer_model(type_id, payload=payload)
        Model.create(**payload)

        # Mutate payload
        new_payload = payload.copy()
        new_payload['field'] = 'bar'

        # FIXME: We need a typed exception here.
        with pytest.raises(Exception):
            Model = infer_model(type_id, new_payload)
            Model.create(**new_payload)
