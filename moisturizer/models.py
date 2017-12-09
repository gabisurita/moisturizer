import datetime
import logging
import uuid

from cassandra.cqlengine import models, columns, management


logger = logging.getLogger("models")
logger.setLevel(logging.DEBUG)


PRIMITIVES = [int, bool, float, str, dict]


def cast_primitive(value):
    if any(isinstance(value, typ) for typ in PRIMITIVES):
        return value
    return str(value)


STORAGE_MAPPER = {
    bool: columns.Boolean(),
    int: columns.BigInt(),
    float: columns.Double(),
    str: columns.Text(),
    datetime.date: columns.Date(),
    datetime.datetime: columns.DateTime(),
}


def infer_value_type(value):
    for native, storage in STORAGE_MAPPER.items():
        if isinstance(value, native):
            return storage


def inspect_schema_change(payload):
    return {key: infer_value_type(value) for key, value
            in payload.items() if value is not None}


class DescriptorModel(models.Model):
    table = columns.Text(primary_key=True)
    fields = columns.Map(columns.Text, columns.Text)

    @property
    def schema(self):
        return {k: getattr(columns, v)() for k, v in self.fields.items()}

    @schema.setter
    def schema(self, schema):
        self.fields = {k: v.__class__.__name__ for k, v in schema.items()}

    @classmethod
    def create(cls, *args, **kwargs):
        created = super().create(*args, **kwargs)
        management.create_keyspace_simple(created.table, replication_factor=2)
        return created

    def serialize(self):
        return {k: cast_primitive(v) for k, v in self.items()}

    def delete(self):
        management.drop_keyspace(self.table)
        return super().delete()


class InferredModel(models.Model):
    id = columns.Text(primary_key=True,
                      default=lambda: str(uuid.uuid4()))
    last_modified = columns.DateTime(index=True,
                                     default=datetime.datetime.now)

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


class ModelInferenceException(Exception):

    def __init__(self, table):
        self.table = table


class ModelNotExists(ModelInferenceException):

    def __str__(self):
        return "Table {} does not exists.".format(self.table)


def create_model(table, payload=None):
    logger.info('Creating new descriptor.', extra={'table': table})
    descriptor = DescriptorModel.create(table=table)
    Model = type('Model', (InferredModel, ), {'__keyspace__': table})
    management.sync_table(Model)
    return descriptor


def infer_model(table, payload=None):
    # First, we attempt to find a descriptor for the current model.
    try:
        descriptor = DescriptorModel.objects.get(table=table)

    # If not exists, check if we can create it.
    except DescriptorModel.DoesNotExist:
        if payload is None:
            raise ModelNotExists(table=table)

        descriptor = create_model(table)

    # With the schema in hands, we create a type for the inferred model.
    Model = type('Model', (InferredModel, ), {'__keyspace__': table})
    Model.apply_schema_change(descriptor.schema)

    # If no changes to apply to the schema, return the inferred model.
    if payload is None:
        return Model

    # Else we need to check for changes and apply them on the schema
    schema = inspect_schema_change(payload)
    diff = {k: v for k, v in schema.items() if not hasattr(Model, k)}

    if not diff:
        return Model

    logger.info('Mutating schema.', extra={'table': table, 'changes': schema})
    Model.apply_schema_change(diff)
    management.sync_table(Model)
    descriptor.schema = Model.get_schema()
    descriptor.save()

    return Model
