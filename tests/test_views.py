import pytest

# rename otherwise pytest will think is a test case
from webtest import TestApp as WebTestApp

from moisturizer import main
from moisturizer.models import UserModel


type_collection = '/types'
type_endpoint = '/types/{}'

object_collection = '/types/my_type/objects'
object_endpoint = '/types/my_type/objects/{}'

user_collection = '/users'
user_endpoint = '/users/{}'

permissions_collection = '/users/{}/permissions'
permissions_endpoint = '/users/{}/permissions/{}'

test_app_configuration = {
    'moisturizer.keyspace': 'test',
}


@pytest.fixture(scope='module')
def test_app():
    app = WebTestApp(main({}, **test_app_configuration))
    return app


@pytest.fixture(scope='module')
def authorized_app():
    app = WebTestApp(main({}, **test_app_configuration))
    admin = UserModel.get(id='admin')
    app.authorization = ('Basic', (admin.id, admin.api_key))
    return app


@pytest.fixture()
def user_auth(authorized_app, test_user):
    base_auth = authorized_app.authorization
    user = UserModel.get(id=test_user['id'])
    authorized_app.authorization = ('Basic', (user.id, user.api_key))
    yield test_user
    authorized_app.authorization = base_auth


@pytest.fixture()
def valid_object_payload():
    return {'foo': 'bar', 'number': 42}


@pytest.fixture()
def invalid_object_payload():
    return {'foo': 12, 'number': 42}


@pytest.fixture()
def valid_type_payload():
    return {
        'id': 'my_type',
        'description': 'My precious type.',
        'properties': {
            'foo': {
                'type': 'string'
            }
        }
    }


@pytest.fixture()
def invalid_type_payload():
    return {'foo': 12, 'number': 42}


@pytest.fixture()
def valid_user_payload():
    return {
        'id': 'my_user',
        'role': 'user',
        'password': 'my_secret'
    }


@pytest.fixture()
def read_only_permissions_payload():
    return {
        'read': True,
        'id': 'my_type'
    }


@pytest.fixture()
def create_only_permissions_payload():
    return {
        'create': True,
        'id': 'my_type'
    }


@pytest.fixture()
def write_only_permissions_payload():
    return {
        'write': True,
        'id': 'my_type'
    }


@pytest.fixture()
def full_permissions_payload():
    return {
        'read': True,
        'create': True,
        'write': True,
        'id': 'my_type'
    }


@pytest.fixture()
def test_object(authorized_app, valid_object_payload):
    return authorized_app.post_json(object_collection,
                                    valid_object_payload,
                                    status=200).json


@pytest.fixture()
def test_type(authorized_app, valid_type_payload):
    return authorized_app.post_json(type_collection,
                                    valid_type_payload,
                                    status=200).json


@pytest.fixture()
def test_user(authorized_app, valid_user_payload):
    return authorized_app.post_json(user_collection,
                                    valid_user_payload,
                                    status=200).json


@pytest.fixture()
def sample_read_only_object(authorized_app, test_object, test_user,
                            read_only_permissions_payload):
    authorized_app.post_json(
        permissions_collection.format(test_user['id']),
        read_only_permissions_payload,
        status=200
    )
    return test_object


@pytest.fixture()
def sample_create_only_object(authorized_app, test_object, test_user,
                              create_only_permissions_payload):
    authorized_app.post_json(
        permissions_collection.format(test_user['id']),
        create_only_permissions_payload,
        status=200
    )
    return test_object


@pytest.fixture()
def sample_write_only_object(authorized_app, test_object, test_user,
                             write_only_permissions_payload):
    authorized_app.post_json(
        permissions_collection.format(test_user['id']),
        write_only_permissions_payload,
        status=200
    )
    return test_object


@pytest.fixture()
def sample_full_permissions_object(authorized_app, test_object, test_user,
                                   full_permissions_payload):
    authorized_app.post_json(
        permissions_collection.format(test_user['id']),
        full_permissions_payload,
        status=200
    )
    return test_object


def test_heartbeat(authorized_app):
    data = authorized_app.get('/__heartbeat__', status=200).json
    assert data['server']
    assert data['schema']
    assert data['users']


def assert_response(payload, reponse):
    for k, v in payload.items():
        assert reponse[k] == v


def test_object_create(authorized_app, valid_object_payload):
    data = authorized_app.post_json(object_collection,
                                    valid_object_payload,
                                    status=200).json
    assert_response(valid_object_payload, data)


def test_object_get(authorized_app, valid_object_payload):
    created = authorized_app.post_json(object_collection,
                                       valid_object_payload,
                                       status=200).json
    data = authorized_app.get(object_endpoint.format(created['id']),
                              status=200).json
    assert_response(valid_object_payload, data)


def test_object_invalid_create(authorized_app, test_object,
                               invalid_object_payload):
    error = authorized_app.post_json(object_collection,
                                     invalid_object_payload,
                                     status=400).json
    assert error


def test_object_list(authorized_app, test_object,
                     valid_object_payload):
    data = authorized_app.get(object_collection, status=200).json
    assert len(data) > 1
    assert_response(valid_object_payload, data[0])


def test_object_list_on_not_existing(authorized_app):
    object_collection = '/types/my_other_nonexisting_type/objects'
    authorized_app.get(object_collection, status=403).json


def test_object_list_on_deleted(authorized_app, test_object,
                                valid_object_payload):
    data = authorized_app.get(object_collection, status=200).json
    assert len(data) > 1
    data = authorized_app.delete(object_collection, status=200).json
    assert_response(valid_object_payload, data[0])
    data = authorized_app.get(object_collection, status=200).json
    assert len(data) == 0


def test_object_update_creates(authorized_app, valid_object_payload):
    data = authorized_app.put_json(object_endpoint.format('42'),
                                   valid_object_payload,
                                   status=200).json

    assert_response(valid_object_payload, data)
    assert data['id'] == '42'


def test_object_update_overwrites(authorized_app, valid_object_payload):
    initial_data = authorized_app.put_json(object_endpoint.format('42'),
                                           valid_object_payload,
                                           status=200).json

    next_payload = valid_object_payload.copy()
    next_payload['banana'] = 'apple'

    data = authorized_app.put_json(object_endpoint.format('42'),
                                   next_payload,
                                   status=200).json

    assert_response(valid_object_payload, data)
    assert data['banana'] == 'apple'
    assert data['last_modified'] > initial_data['last_modified']


def test_object_invalid_update(authorized_app,
                               valid_object_payload,
                               invalid_object_payload):

    authorized_app.put_json(object_endpoint.format('42'),
                            valid_object_payload,
                            status=200).json

    authorized_app.put_json(object_endpoint.format('42'),
                            invalid_object_payload,
                            status=400).json


def test_object_patch_edits(authorized_app, valid_object_payload):
        initial_data = authorized_app.put_json(object_endpoint.format('42'),
                                               valid_object_payload,
                                               status=200).json

        next_payload = valid_object_payload.copy()
        next_payload = {'banana': 'apple'}

        data = authorized_app.patch_json(object_endpoint.format('42'),
                                         next_payload,
                                         status=200).json

        assert_response(valid_object_payload, data)
        assert data['banana'] == 'apple'
        assert data['last_modified'] > initial_data['last_modified']


def test_object_delete(authorized_app, valid_object_payload):
        authorized_app.put_json(object_endpoint.format('42'),
                                valid_object_payload,
                                status=200)
        data = authorized_app.delete(object_endpoint.format('42')).json
        assert_response(valid_object_payload, data)


def test_type_create(authorized_app, valid_type_payload):
    data = authorized_app.post_json(type_collection,
                                    valid_type_payload,
                                    status=200).json
    assert_response(valid_type_payload, data)


def test_type_validation(authorized_app, test_type,
                         valid_object_payload, invalid_object_payload):

    # Try to save with invalid schema
    authorized_app.post_json(object_collection,
                             invalid_object_payload,
                             status=400).json

    # Try to save with valid schema
    authorized_app.post_json(object_collection,
                             valid_object_payload,
                             status=200).json


@pytest.mark.skip
def test_type_migration(authorized_app, valid_type_payload,
                        valid_object_payload, invalid_object_payload):
    # Infer wrong schema type
    authorized_app.post_json(object_collection,
                             invalid_object_payload,
                             status=200).json

    # Migrate type
    authorized_app.put_json(type_endpoint,
                            valid_type_payload,
                            status=200).json

    # Insert with corrent schema type
    authorized_app.post_json(object_collection,
                             valid_object_payload,
                             status=200).json


def test_admin_get(authorized_app):
    data = authorized_app.get(user_endpoint.format('admin')).json
    assert data.get('api_key')
    assert not data.get('password')
    assert not data.get('_password')


def test_user_create(authorized_app, valid_user_payload):
    data = authorized_app.post_json(user_collection,
                                    valid_user_payload,
                                    status=200).json

    assert data.get('api_key')


def test_user_list(authorized_app, test_user, valid_user_payload):
    data = authorized_app.get(user_collection, status=200).json
    assert len(data) > 0


def test_user_delete(authorized_app, test_user, valid_user_payload):
    data = authorized_app.delete(user_endpoint.format(test_user['id'])).json
    assert data.get('api_key') == test_user['api_key']


def test_type_permissions_filter(authorized_app, test_object, user_auth):
    authorized_app.get(object_collection, status=403)
    authorized_app.get(object_collection.format(test_object['id']),
                       status=403)


def test_type_permissions_create(authorized_app, test_user,
                                 read_only_permissions_payload):
    data = authorized_app.post_json(
        permissions_collection.format(test_user['id']),
        read_only_permissions_payload,
        status=200
    ).json

    assert data['read']
    assert not data['create']
    assert not data['write']


def test_type_permissions_access_read(authorized_app,
                                      sample_read_only_object,
                                      user_auth):

    data = authorized_app.get(object_collection, status=200).json
    assert len(data) > 0

    authorized_app.post_json(object_collection, {}, status=403)
    authorized_app.put_json(
        object_endpoint.format(sample_read_only_object['id']),
        {}, status=403
    )
    authorized_app.patch_json(
        object_endpoint.format(sample_read_only_object['id']),
        {}, status=403
    )


@pytest.mark.skip
def test_type_permissions_access_create(authorized_app,
                                        sample_create_only_object,
                                        user_auth):

    authorized_app.get(object_collection, status=403)
    authorized_app.post_json(object_collection, {}, status=200)
    authorized_app.put_json(
        object_endpoint.format(sample_create_only_object['id']),
        {}, status=403
    )
    authorized_app.patch_json(
        object_endpoint.format(sample_create_only_object['id']),
        {}, status=403
    )


def test_type_permissions_access_write(authorized_app,
                                       sample_write_only_object,
                                       user_auth):

    authorized_app.get(object_collection, status=403)
    authorized_app.post_json(object_collection, {}, status=200)
    authorized_app.put_json(
        object_endpoint.format(sample_write_only_object['id']),
        {}, status=200
    )
    authorized_app.patch_json(
        object_endpoint.format(sample_write_only_object['id']),
        {}, status=200
    )
