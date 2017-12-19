import os
import logging

from pyramid.config import Configurator, ConfigurationError
from cassandra.cqlengine import connection, management, query

from moisturizer.models import (
    DescriptorModel,
    DescriptorFieldType,
    UserModel,
    PermissionModel
)


REQUIRED_SETTINGS = [
]

DEFAULT_SETTINGS = {
    'moisturizer.cassandra_cluster': '0.0.0.0',
    'moisturizer.keyspace': 'moisturizer',

    'moisturizer.read_only': False,
    'moisturizer.immutable_schema': False,
    'moisturizer.strict_schema': False,

    'moisturizer.create_keyspace': True,
    'moisturizer.override_keyspace': False,

    'moisturizer.admin_id': 'admin',
    'moisturizer.admin_password': 'admin',
}


logger = logging.getLogger('moisturizer')


def get_config_environ(name):
    env_name = name.replace('.', '_').upper()
    return os.environ.get(env_name)


def allow_migratation(settings):
    return (not settings['moisturizer.read_only'] and
            not settings['moisturizer.immutable_schema'])


def allow_inference_migration(settings):
    return (allow_migratation(settings) and
            not settings['moisturizer.strict_schema'])


def migrate_metaschema(settings):
    """Creates if not exists the Table descriptor Model and ajust
    it to the current schema."""

    admin_id = settings['moisturizer.admin_id']
    admin_password = settings['moisturizer.admin_id']
    override_keyspace = settings['moisturizer.override_keyspace']

    if override_keyspace:
        management.drop_keyspace(settings['moisturizer.keyspace'])

    management.create_keyspace_simple(settings['moisturizer.keyspace'],
                                      replication_factor=1)

    # Create users
    management.sync_table(UserModel)

    # Create permissions
    management.sync_table(PermissionModel)

    # Create admin
    try:
        UserModel.objects.if_not_exists().create(
            id=admin_id,
            password=admin_password,
            role=UserModel.ROLE_ADMIN,
        )
    except query.LWTException:
        pass

    # Create descriptors
    management.sync_table(DescriptorModel)

    DescriptorModel.create(id='descriptor_model', properties={
        'properties': DescriptorFieldType(type='object',
                                          format='descriptor'),
    })
    DescriptorModel.create(id='user_model', properties={
        'api_key': DescriptorFieldType(type='string',
                                       format='uuid'),
    })
    # DescriptorModel.create(id=PermissionModel.__keyspace__)


def main(global_config, **settings):
    for name, value in DEFAULT_SETTINGS.items():
        settings.setdefault(name, get_config_environ(name) or value)

    for name in REQUIRED_SETTINGS:
        if settings.get(name) is None:
            error = 'confiration entry for {} is missing'.format(name)
            logger.critical(error)
            raise ConfigurationError(error)

    config = Configurator(settings=settings)
    config.include("cornice")
    config.include("moisturizer.auth")
    config.scan("moisturizer.views")

    connection.setup([settings['moisturizer.cassandra_cluster']],
                     settings['moisturizer.keyspace'],
                     protocol_version=3)

    migrate = allow_migratation(settings)

    os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = str(migrate)

    if migrate:
        migrate_metaschema(settings)

    return config.make_wsgi_app()
