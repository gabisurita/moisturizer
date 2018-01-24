import os
import logging
import asyncio

from aiocassandra import aiosession
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.cqlengine import connection, management

from moisturizer.models import (
    DescriptorModel,
    DescriptorFieldType,
)
from moisturizer.consumer import MoisturizerKafkaConsumer

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('moisturizer')


def migrate(settings):
    """Creates if not exists the Table descriptor Model and ajust
    it to the current schema."""

    override_keyspace = settings['cassandra.override_keyspaces']

    if override_keyspace:
        management.drop_keyspace(settings['cassandra.keyspace_default'])

    management.create_keyspace_simple(settings['cassandra.keyspace_default'],
                                      replication_factor=1)

    management.sync_table(DescriptorModel)

    DescriptorModel.create(id='descriptor_model', properties={
        'properties': DescriptorFieldType(type='object',
                                          format='descriptor'),
    })


def async_start(settings, cassandra_session):

    loop = asyncio.get_event_loop()
    # loop.set_debug(True)

    consumer = MoisturizerKafkaConsumer(
        cluster=settings.get('kafka.cluster'),
        topics=settings.get('kafka.topics').split(','),
        group=settings.get('kafka.group'),
        event_loop=loop,
    )

    aiosession(cassandra_session, loop=loop)
    loop.run_until_complete(consumer.start())


def main(settings):
    cluster = Cluster([settings['cassandra.cluster']])
    session = cluster.connect(settings['cassandra.keyspace_default'])
    session.row_factory = dict_factory
    connection.set_session(session)

    allow_migration = not settings['cassandra.immutable_schema']

    # Prevent CQL engine migration warnings.
    os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = str(allow_migration)

    if allow_migration:
        migrate(settings)

    logger.info("Starting consumer async loop.")
    return async_start(settings, session)
