"""Main entry point
"""
import os
import logging

import logmatic
from pyramid.config import Configurator, ConfigurationError
from cassandra.cqlengine import connection, management

from moisturizer.models import DescriptorModel


REQUIRED_SETTINGS = [
]

ENV_SETTINGS = [
    'moisturizer.cassandra_cluster',
]


logger = logging.getLogger('moisturizer')
handler = logging.StreamHandler()
handler.setFormatter(logmatic.JsonFormatter())

logger.addHandler(handler)
logger.setLevel(logging.INFO)


def get_config_environ(name):
    env_name = name.replace('.', '_').upper()
    return os.environ.get(env_name)


def main(global_config, **settings):
    for name in ENV_SETTINGS:
        settings[name] = get_config_environ(name) or settings.get(name)

    for name in REQUIRED_SETTINGS:
        if settings.get(name) is None:
            error = 'confiration entry for {} is missing'.format(name)
            logger.critical(error)
            raise ConfigurationError(error)

    config = Configurator(settings=settings)
    config.include("cornice")
    config.scan("moisturizer.views")

    connection.setup(['127.0.0.1'], "test", protocol_version=3)
    management.create_keyspace_simple('table_descriptor', replication_factor=2)
    management.sync_table(DescriptorModel)

    return config.make_wsgi_app()
