"""Main entry point
"""
import os
import logging

import logmatic
from pyramid.config import Configurator as BaseConfigurator, ConfigurationError
from cassandra.cqlengine import connection, management

from moisturizer.models import DescriptorModel


REQUIRED_SETTINGS = [
]

DEFAULT_SETTINGS = {
    'moisturizer.cassandra_cluster': ['0.0.0.0'],
    'moisturizer.keyspace': 'moisturizer',
    'moisturizer.descriptor_key': '__table_descriptor__',

    'moisturizer.read_only': False,
    'moisturizer.immutable_schema': False,
    'moisturizer.strict_schema': False,

    'moisturizer.create_keyspace': True,
    'moisturizer.override_keyspace': False,
}


logger = logging.getLogger('moisturizer')
handler = logging.StreamHandler()
handler.setFormatter(logmatic.JsonFormatter())

logger.addHandler(handler)
logger.setLevel(logging.INFO)


class Configurator(BaseConfigurator):
    pass


def get_config_environ(name):
    env_name = name.replace('.', '_').upper()
    return os.environ.get(env_name)


def allow_migratation(settings):
    return (not settings['moisturizer.read_only'] and
            not settings['moisturizer.immutable_schema'])


def allow_inference_migration(settings):
    return (allow_migratation(settings) and
            not settings['moisturizer.strict_schema'])


def migrate_descriptor(settings):
    """Creates if not exists the Table descriptor Model and ajust
    it to the current schema."""

    descriptor_key = settings['moisturizer.descriptor_key']

    management.create_keyspace_simple(descriptor_key, replication_factor=2)
    DescriptorModel.__keyspace__ = descriptor_key
    management.sync_table(DescriptorModel)
    DescriptorModel.create(table=descriptor_key)


def main(global_config, **settings):
    for name, value in DEFAULT_SETTINGS.items():
        settings[name] = (get_config_environ(name) or
                          settings.get(name) or value)

    for name in REQUIRED_SETTINGS:
        if settings.get(name) is None:
            error = 'confiration entry for {} is missing'.format(name)
            logger.critical(error)
            raise ConfigurationError(error)

    config = Configurator(settings=settings)
    config.include("cornice")
    config.scan("moisturizer.views")

    connection.setup(settings['moisturizer.cassandra_cluster'],
                     settings['moisturizer.keyspace'],
                     protocol_version=3)

    migrate = allow_migratation(settings)

    os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = str(migrate)

    if migrate:
        migrate_descriptor(settings)

    return config.make_wsgi_app()