import datetime
import logging
import uuid

from cassandra.cqlengine import models, columns, management


logger = logging.getLogger("models")
logger.setLevel(logging.DEBUG)


PRIMITIVES = [int, bool, float, str]


def cast_primitive(value):
    if any(isinstance(value, typ) for typ in PRIMITIVES):
        return value

    return str(value)


class DescriptorModel(models.Model):
    __keyspace__ = 'table_descriptor'

    table = columns.Text(primary_key=True)
    fields = columns.Map(columns.Text, columns.Text)

    def get_schema(self):
        return {k: getattr(columns, v)() for k, v in self.fields.items()}

    def set_schema(self, schema):
        self.fields = {k: v.__class__.__name__ for k, v in schema.items()}


class InferredModel(models.Model):
    id = columns.UUID(primary_key=True, default=uuid.uuid4)
    last_modified = columns.DateTime(default=datetime.datetime.now)

    @classmethod
    def apply_schema_change(cls, schema):
        for k, v in schema.items():
            if not hasattr(cls, k):
                cls.add_column(k, v)

    @classmethod
    def add_column(cls, name, column_type):
        column_type.column_name = name
        descriptor = models.ColumnDescriptor(column_type)

        cls._defined_columns[name] = column_type
        cls._columns[name] = column_type
        setattr(cls, name, descriptor)

    @classmethod
    def get_schema(cls):
        return cls._columns

    def serialize(self):
        return {k: cast_primitive(v) for k, v in self.items()}


def inspect_schema_change(payload):
    return {key: columns.Text() for key, value
            in payload.items() if value is not None}


def infer_model(table, payload=None):
    try:
        descriptor = DescriptorModel.objects.get(table=table)

    except DescriptorModel.DoesNotExist:
        logger.info('Creating new descriptor.', extra={'table': table})
        descriptor = DescriptorModel.create(table=table)
        management.create_keyspace_simple(table, replication_factor=2)

    Model = type('Model', (InferredModel, ), {'__keyspace__': table})
    Model.apply_schema_change(descriptor.get_schema())

    if payload is None:
        return Model

    schema = inspect_schema_change(payload)
    diff = {k: v for k, v in schema.items() if not hasattr(Model, k)}

    if not diff:
        return Model

    logger.info('Mutating schema.', extra={'table': table, 'changes': schema})
    management.create_keyspace_simple(table, replication_factor=2)
    Model.apply_schema_change(diff)
    management.sync_table(Model)
    descriptor.set_schema(Model.get_schema())
    descriptor.save()

    return Model
