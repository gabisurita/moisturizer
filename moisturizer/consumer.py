import asyncio
import json
import msgpack

from kafka import KafkaConsumer

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

    def unwrap_message(self, raw_value):
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

    def get_descriptor(self, type_id):
        cached = self.descriptors.get(type_id)
        if cached is not None:
            return cached

        return self.load_descriptor(type_id)

    def load_descriptor(self, type_id):
        try:
            descriptor = DescriptorModel.get(id=type_id)
        except DescriptorModel.DoesNotExist as e:
            descriptor = DescriptorModel.create(id=type_id)

        self.descriptors[type_id] = descriptor
        return descriptor

    def commit_message(self, message):
        type_, payload = self.unwrap_message(message)
        descriptor = self.get_descriptor(type_)

        schema = self.schema.bind(descriptor=descriptor)
        deserialized = schema.deserialize(payload)
        flatten = schema.flatten(deserialized)

        changes = descriptor.infer_schema_change(flatten)
        if changes:
            descriptor = self.load_descriptor(type_)

        model = descriptor.model(**flatten)
        model.save()

    async def start(self):
        consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=self.cluster,
            group_id=self.group,
        )
        # Consume messages
        for message in consumer:
            try:
                await asyncio.ensure_future(self.commit_message(message.value))
            except Exception as e:
                raven.captureException()
