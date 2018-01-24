import os
import logging


DEFAULT_SETTINGS = {
    'kafka.cluster': '0.0.0.0:9092',
    'kafka.topics': 'test',
    'kafka.group': 'moisturizer',

    'cassandra.cluster': '0.0.0.0',
    'cassandra.keyspace_default': 'moisturizer',

    'cassandra.create_keyspaces': True,
    'cassandra.override_keyspaces': False,
    'cassandra.immutable_schema': False,
}


REQUIRED_SETTINGS = [
]


logger = logging.getLogger('moisturizer')


def get_config_environ(name):
    env_name = name.replace('.', '_').upper()
    return os.environ.get(env_name)


def load_settings(**settings):
    for name, value in DEFAULT_SETTINGS.items():
        settings.setdefault(name, get_config_environ(name) or value)

    for name in REQUIRED_SETTINGS:
        if settings.get(name) is None:
            error = 'confiration entry for {} is missing'.format(name)
            logger.critical(error)
            raise ValueError(error)

    return settings


settings = load_settings()
