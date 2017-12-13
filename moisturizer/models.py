import bcrypt
import datetime
import logging
import uuid

from cassandra.cqlengine import models, columns, management, usertype
from pyramid.decorator import reify

from moisturizer.exceptions import ModelNotExists
from moisturizer.utils import cast_primitive


NATIVE_JSONSCHEMA_TYPE_MAPPER = {
    bool: ('boolean', None),
    int: ('integer', None),
    float: ('number', None),
    str: ('string', None),
}

JSONSCHEMA_CQL_TYPE_MAPPER = {
    ('string', None): columns.Text,
    ('number', None): columns.Decimal,
    ('integer', None): columns.Integer,
    ('boolean', None): columns.Boolean,
    ('null', None): lambda **_: None,
    ('object', None): lambda **kwargs: columns.Map(columns.String,
                                                   columns.String,
                                                   **kwargs),
    ('array', None): lambda **kwargs: columns.list(columns.String,
                                                   **kwargs),
    ('string', 'date-time'): columns.DateTime,
    ('string', 'uuid'): columns.UUID,
    ('number', 'float'): columns.Float,
    ('number', 'double'): columns.Double,
}


CQL_JSONSCHEMA_TYPE_MAPPER = {
    v: k for v, k in JSONSCHEMA_CQL_TYPE_MAPPER
}

DEFAULT_CQL_TYPE = columns.Text


logger = logging.getLogger("moisturizer.models")
logger.setLevel(logging.DEBUG)


class InferredModel(models.Model):
    """
    Abstract class for inferred type models.

    Creates a typed object with the ``infer_model()`` call or by
    extending and creating custom inferred models.
    """
    id = columns.Text(primary_key=True,
                      default=lambda: str(uuid.uuid4()))
    last_modified = columns.DateTime(index=True,
                                     default=datetime.datetime.now)

    @classmethod
    def add_column(cls, name, column_type):
        column_type.column_name = name
        descriptor = models.ColumnDescriptor(column_type)

        cls._defined_columns[name] = column_type
        cls._columns[name] = column_type
        setattr(cls, name, descriptor)

    @classmethod
    def from_descriptor(cls, descriptor):
        """Builds an InferredModel child class from a descriptor object.
        """

        Model = type('Model', (cls, ), {'__keyspace__': descriptor.id})
        for name, field in descriptor.properties.items():

            # Ignore explicitly declared fields
            if not getattr(cls, name, None):
                Model.add_column(name, field.as_column())

        return Model


class DescriptorFieldType(usertype.UserType):
    type = columns.Text()
    format = columns.Text(default='')
    primary_key = columns.Boolean(default=False)
    partition_key = columns.Boolean(default=False)
    required = columns.Boolean(default=False)
    index = columns.Boolean(default=False, db_field='index_')

    @classmethod
    def from_value(cls, value):
        for native, field in NATIVE_JSONSCHEMA_TYPE_MAPPER.items():
            if isinstance(value, native):
                type_, format_ = field
                return cls(type=type_, format=format_ or '')

    def as_column(self):
        type_, format_ = self.type, self.format or None
        field = JSONSCHEMA_CQL_TYPE_MAPPER.get((type_, format_),
                                               DEFAULT_CQL_TYPE)
        return field(
            primary_key=self.primary_key,
            partition_key=self.partition_key,
            index=self.index_,
            required=self.required,
        )


class DescriptorModel(InferredModel):
    properties = columns.Map(columns.Text,
                             columns.UserDefinedType(DescriptorFieldType))

    def __init__(self, *args,  **kwargs):
        super().__init__(*args, **kwargs)
        self.properties.update(**{
            'id': DescriptorFieldType(type='string',
                                      format='',
                                      primary_key=True,
                                      partition_key=True),
            'last_modified': DescriptorFieldType(type='string',
                                                 format='date-time',
                                                 index=True)
        })

    @property
    def schema(self):
        return {k: v.as_column() for k, v in self.properties.items()}

    def infer_schema_change(self, object_):
        new_fields = {k: DescriptorFieldType.from_value(v)
                      for k, v in object_.items() if k not in self.properties}

        if not new_fields:
            return

        logger.info('Mutating schema.', extra={
            'type_id': self.id,
        })

        self.properties.update(**new_fields)  # noqa
        return new_fields

    @classmethod
    def create(cls, *args, **kwargs):
        created = super().create(*args, **kwargs)
        management.create_keyspace_simple(created.id, replication_factor=1)
        return created

    def delete(self):
        management.drop_keyspace(self.id)
        return super().delete()


class UserModel(InferredModel):
    ROLE_USER = 'user'
    ROLE_ADMIN = 'admin'

    _password = columns.Ascii(required=True)
    api_key = columns.UUID(default=uuid.uuid4)
    role = columns.Text(default=ROLE_USER, index=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        plain_password = kwargs.get('password')
        self.process_plain_password(plain_password)

    def create(self, *args, **kwargs):
        super().create(*args, **kwargs)
        plain_password = kwargs.get('password')
        self.process_plain_password(plain_password)

    def process_plain_password(self, plain_password):
        if plain_password:
            hashed = bcrypt.hashpw(plain_password.encode('utf-8'),
                                   bcrypt.gensalt())
            self._password = hashed.decode('utf-8')

    def check_password(self, given_password):
        return bcrypt.checkpw(given_password.encode('utf-8'),
                              self._password.encode('utf-8'))

    def serialize(self):
        return {k: cast_primitive(v) for k, v in self.items()
                if k != '_password'}


class PermissionModel(models.Model):
    __keyspace__ = '__permissions__'

    id = columns.Text(partition_key=True)
    type_id = columns.Text(partition_key=True)
    owner = columns.Text(partition_key=True)

    can_read = columns.Boolean(default=False)
    can_create = columns.Boolean(default=False)
    can_update = columns.Boolean(default=False)
    can_delete = columns.Boolean(default=False)

    @reify
    def admin(self):
        return PermissionModel(
            can_read=True,
            can_create=True,
            can_update=True,
            can_delete=True,
        )

    def serialize(self):
        return {k: cast_primitive(v) for k, v in self.items()}


def create_model(type_id, payload=None):
    logger.info('Creating new descriptor.', extra={'type_id': type_id})
    descriptor = DescriptorModel.create(id=type_id)

    # Force the first sync.
    Model = type('Model', (InferredModel, ), {'__keyspace__': type_id})
    management.sync_table(Model)

    return descriptor


def infer_value_type(value):
    for native, type_ in NATIVE_JSONSCHEMA_TYPE_MAPPER.items():
        if isinstance(value, native):
            return JSONSCHEMA_CQL_TYPE_MAPPER.get(type_)


def inspect_schema_change(payload):
    return {key: infer_value_type(value) for key, value
            in payload.items() if value is not None}


def infer_model(type_id, payload=None):
    """This is where magic happens!"""

    # First, we attempt to find a descriptor for the current model.
    try:
        descriptor = DescriptorModel.get(id=type_id)

    # If not exists, check if we can create it.
    except DescriptorModel.DoesNotExist:
        if payload is None:
            raise ModelNotExists(type_id=type_id)

        descriptor = DescriptorModel(id=type_id)

    if payload is None:
        return InferredModel.from_descriptor(descriptor)

    has_change = descriptor.infer_schema_change(payload)
    Model = InferredModel.from_descriptor(descriptor)

    if has_change:
        descriptor.save()
        management.sync_table(Model)

    return Model
