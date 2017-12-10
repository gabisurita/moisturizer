import os

import pytest
from cassandra.cqlengine import connection

from moisturizer import (
    migrate_metaschema,
    DEFAULT_SETTINGS,
)
from moisturizer.models import (
    DescriptorModel,
    infer_model,
)

connection.setup(['127.0.0.1'],
                 'test_keyspace',
                 protocol_version=3)


@pytest.fixture(scope="module", autouse=True)
def setup_migrations():
    os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = str(True)
    yield migrate_metaschema(DEFAULT_SETTINGS)


@pytest.fixture()
def infer_model_fixture(table, payload):
    Model = infer_model(table, payload)
    yield Model
    try:
        DescriptorModel.get(table=table).delete()
    except:
        pass


@pytest.mark.parametrize("table, payload", [
    ('hello', {}),
    ('hello', {'field': 'foo'}),
    ('hello', {'field': ''}),
    ('hello', {'field': 0}),
    ('hello', {'field': 42}),
    ('hello', {'field': 42.42}),
    ('hello', {'field': True}),
    ('hello', {'field': False}),
])
class TestModelInference(object):

    def test_type_inference(self, infer_model_fixture, table, payload):
        Model = infer_model_fixture
        created = Model.create(**payload)
        assert getattr(created, 'field', None) == payload.get('field')

    def test_model_mutation(self, infer_model_fixture, table, payload):
        # Infer once using payload
        Model = infer_model_fixture

        # Mutate payload
        new_payload = payload.copy()
        new_payload['field2'] = 'bar'
        Model = infer_model(table, new_payload)
        created = Model.create(**new_payload)

        assert getattr(created, 'field', None) == new_payload.get('field')
        assert getattr(created, 'field2', None) == new_payload.get('field2')

    def test_invalid_model_mutation(self, infer_model_fixture, table, payload):
        field = payload.get('field')

        # FIXME: Bool doesn't seem quite right here
        if field is None or isinstance(field, str) or isinstance(field, bool):
            return

        # Infer once using payload
        Model = infer_model_fixture
        Model.create(**payload)

        # Mutate payload
        new_payload = payload.copy()
        new_payload['field'] = 'bar'

        # FIXME: We need a typed exception here.
        with pytest.raises(Exception):
            Model = infer_model(table, new_payload)
            Model.create(**new_payload)
