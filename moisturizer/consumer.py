import asyncio
import json
import msgpack

from aiokafka import AIOKafkaConsumer

from moisturizer.models import DescriptorModel
from moisturizer.schemas import InferredObjectSchema
from moisturizer.config import raven


class MoisturizerKafkaConsumer:

    _loop = None
    descriptors = {}
    schema = InferredObjectSchema()

    def __init__(self, cluster, topics, group, event_loop):
        self.cluster = cluster
        self.topics = topics
        self.group = group
        self._loop = event_loop

    async def unwrap_message(self, message):
        raw_value = message.value

        # Try to decode MsgPack
        try:
            payload = msgpack.loads(raw_value, encoding='utf-8')

        # Try to decode JSON
        except msgpack.exceptions.UnpackException:
            payload = json.loads(raw_value)

        type_ = payload.get('type_id')
        if type_ is None:
            raise ValueError("Object type was not provided.")

        data = payload.get('data') or {}

        return type_, data

    async def get_descriptor(self, type_id):
        cached = self.descriptors.get(type_id)
        if cached is not None:
            return cached

        return await self.load_descriptor(type_id)

    async def load_descriptor(self, type_id):
        try:
            descriptor = DescriptorModel.get(id=type_id)
        except DescriptorModel.DoesNotExist as e:
            descriptor = DescriptorModel.create(id=type_id)

        self.descriptors[type_id] = descriptor
        return descriptor

    async def commit_message(self, message):
        type_, payload = await self.unwrap_message(message)
        descriptor = await self.get_descriptor(type_)

        schema = self.schema.bind(descriptor=descriptor)
        deserialized = schema.deserialize(payload)
        flatten = schema.flatten(deserialized)

        changes = descriptor.infer_schema_change(flatten)
        if changes:
            descriptor = await self.load_descriptor(type_)

        model = descriptor.model(**flatten)
        model.save()

    async def start(self):
        consumer = AIOKafkaConsumer(
            *self.topics,
            loop=self._loop,
            bootstrap_servers=self.cluster,
            group_id=self.group,
        )

        await consumer.start()
        try:
            # Consume messages
            async for message in consumer:
                try:
                    asyncio.ensure_future(self.commit_message(message))
                except Exception as e:
                    raven.captureException()
        finally:
            # Will leave consumer group; perform autocommit if enabled.
            await consumer.stop()
