import datetime

import colander
import mock
import pytest

from moisturizer.schemas import InferredObjectSchema, InferredTypeSchema


@pytest.fixture()
def freeze_now(scope="module"):
    value = datetime.datetime.now()
    yield value


@pytest.fixture()
def object_payload(freeze_now):
    return {
        'id': 'MyType',
        'last_modified': str(freeze_now),
        'foo': '42',
    }


@pytest.fixture()
def invalid_object_payload():
    return {
        'id': 'MyType',
        'last_modified': 'a long time ago',
    }


@pytest.fixture()
def type_payload(object_payload, freeze_now):
    return {
        'properties': {
            'foo': {
                'type': 'string',
                'index': True,
            }
        },
        **object_payload  # noqa
    }


@pytest.fixture()
def invalid_type_payload():
    return {
        'properties': {
            'foo': {
                'type': 'pancakes',
                'index': 42,
            }
        },
    }


class TestInferredSchema(object):

    def test_free_schema(self, object_payload, freeze_now):
        result = InferredObjectSchema().deserialize({})
        expected = {
            'id': result['id'],
            'last_modified': freeze_now,
            'foo': '42',
        }
        assert result == expected

    def test_validate_known_fields(self, invalid_object_payload):
        with pytest.raises(colander.Invalid):
            InferredObjectSchema().deserialize(invalid_object_payload)

    def test_defer_fields(self, object_payload, freeze_now):
        schema = InferredSchema().bind(
            fields={
                'foo': colander.SchemaNode(colander.String())
            }
        )
        result = schema.deserialize(object_payload)
        expected = {
            'id': result['id'],
            'last_modified': freeze_now,
            'foo': '42',
        }
        assert result == expected

        invalid = {**expected}
        invalid['foo'] = True
        with pytest.raises(colander.Invalid):
            result = schema.deserialize(invalid)


class TestTypeSchema(object):

    def test_serialize_type_schema(self, type_payload, freeze_now):
        result = InferredTypeSchema().deserialize(type_payload)
        expected = {
            'id': result['id'],
            'last_modified': freeze_now,
            'foo': '42',
            'properties': {
                'foo': {
                    'type': 'string',
                    'index': True,
                }
            },
        }
        assert result == expected

    def test_validate_type_schema(self, invalid_type_payload):
        with pytest.raises(colander.Invalid):
            InferredTypeSchema().deserialize(invalid_type_payload)
