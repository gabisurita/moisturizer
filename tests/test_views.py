import pytest
from webtest import TestApp as WebTestApp  # fix pytest import issue

from moisturizer import main
from moisturizer.models import DescriptorModel, UserModel, PermissionModel


@pytest.fixture(scope="module", autouse=True)
def test_app():
    app = WebTestApp(main({}))
    app.authorization = ('Basic', ('admin', 'admin'))
    return app


@pytest.fixture(autouse=True)
def clean_infer_models():
    yield None
    models = DescriptorModel.all()
    ignores = map(lambda m: m.__keyspace__,
                  (DescriptorModel, UserModel, PermissionModel))

    for model in models:
        if model.id not in ignores:
            model.delete()


@pytest.fixture()
def valid_object_payload():
    return {'foo': 'bar', 'number': 42}


@pytest.fixture()
def invalid_object_payload():
    return {'foo': 12, 'number': 42}


@pytest.fixture()
def valid_type_payload():
    return {'foo': 'bar', 'number': 42}


@pytest.fixture()
def invalid_type_payload():
    return {'foo': 12, 'number': 42}


def test_heartbeat(test_app):
    data = test_app.get('/__heartbeat__', status=200).json
    assert data['server']
    assert data['schema']
    assert data['users']


collection_endpoint = '/types/my_type/objects'
object_endpoint = '/types/my_type/objects/{}'


def assert_response(payload, reponse):
    for k, v in payload.items():
        assert reponse[k] == v


def test_object_create(test_app, valid_object_payload):
    data = test_app.post_json(collection_endpoint,
                              valid_object_payload,
                              status=200).json
    assert_response(valid_object_payload, data)


def test_object_get(test_app, valid_object_payload):
    created = test_app.post_json(collection_endpoint,
                                 valid_object_payload,
                                 status=200).json
    object_endpoint = '/types/my_type/objects/{}'.format(created['id'])
    data = test_app.get(object_endpoint, status=200).json
    assert created == data


def test_object_invalid_create(test_app,
                               valid_object_payload,
                               invalid_object_payload):
    test_app.post_json(collection_endpoint,
                       valid_object_payload,
                       status=200).json

    data = test_app.post_json(collection_endpoint,
                              invalid_object_payload,
                              status=400).json

    assert data


def test_object_list(test_app, valid_object_payload):
    test_app.post_json(collection_endpoint,
                       valid_object_payload,
                       status=200).json

    data = test_app.get(collection_endpoint, status=200).json
    assert len(data) == 1
    assert_response(valid_object_payload, data[0])


def test_object_list_on_not_existing(test_app):
    data = test_app.get(collection_endpoint, status=404).json
    assert data


def test_object_list_on_deleted(test_app, valid_object_payload):
    test_app.post_json(collection_endpoint,
                       valid_object_payload,
                       status=200)
    data = test_app.get(collection_endpoint, status=200).json
    assert len(data) == 1
    data = test_app.delete(collection_endpoint, status=200).json
    assert_response(valid_object_payload, data[0])
    data = test_app.get(collection_endpoint, status=200).json
    assert len(data) == 0


def test_object_update_creates(test_app, valid_object_payload):
    data = test_app.put_json(object_endpoint,
                             valid_object_payload,
                             status=200).json

    assert_response(valid_object_payload, data)
    assert data['id'] == '42'


def test_object_update_overwrites(test_app, valid_object_payload):
    initial_data = test_app.put_json(object_endpoint,
                                     valid_object_payload,
                                     status=200).json

    next_payload = valid_object_payload.copy()
    next_payload['banana'] = 'apple'

    data = test_app.put_json(object_endpoint,
                             next_payload,
                             status=200).json

    assert_response(valid_object_payload, data)
    assert data['banana'] == 'apple'
    assert data['last_modified'] > initial_data['last_modified']


def test_object_invalid_update(test_app,
                               valid_object_payload,
                               invalid_object_payload):

    test_app.post_json(collection_endpoint,
                       valid_object_payload,
                       status=200).json

    test_app.put_json(object_endpoint,
                      invalid_object_payload,
                      status=400).json


def test_object_patch_edits(test_app, valid_object_payload):
        initial_data = test_app.put_json(object_endpoint,
                                         valid_object_payload,
                                         status=200).json

        next_payload = valid_object_payload.copy()
        next_payload = {'banana': 'apple'}

        data = test_app.patch_json('/types/my_type/objects/42',
                                   next_payload,
                                   status=200).json

        assert_response(valid_object_payload, data)
        assert data['banana'] == 'apple'
        assert data['last_modified'] > initial_data['last_modified']
