import colander

from moisturizer.models import (
    DescriptorFieldType,
)
from moisturizer.utils import (
    merge_dicts,
    flatten_dict,
    unflatten_dict,
)


JSONSCHEMA_COLANDER_TYPE_MAPPER = {
    ('string', None): colander.String,
    ('number', None): colander.Decimal,
    ('integer', None): colander.Integer,
    ('boolean', None): colander.Boolean,
    ('null', None): lambda **_: None,
    ('string', 'date-time'): colander.DateTime,

    ('string', 'uuid'): lambda **kwargs:
        colander.String(**kwargs),

    ('number', 'float'): colander.Float,
    ('number', 'double'): colander.Float,

    ('object', None): lambda **kwargs:
        colander.Mapping(unknown='preserve', **kwargs),

    ('object', 'descriptor'): lambda **kwargs:
        colander.Mapping(unknown='preserve', **kwargs),

    ('array', None): lambda **kwargs:
        colander.Sequence(colander.String, **kwargs),
}


valid_http_method = colander.OneOf(('GET', 'HEAD', 'DELETE', 'TRACE',
                                    'POST', 'PUT', 'PATCH'))


valid_types = colander.OneOf(('string', 'number', 'object',
                              'array', 'boolean', 'null'))


def _check_string_values(node, cstruct):
    """validate that a ``colander.mapping`` only has strings in its values."""

    are_strings = [isinstance(v, str) for v in cstruct.values()]
    if not all(are_strings):
        error_msg = '{} contains non string value'.format(cstruct)
        raise colander.invalid(node, error_msg)


class BaseMappingSchema(colander.MappingSchema):
    """Base schema to (de)serialize objects inherited from BaseResource."""

    def schema_type(self):
        return colander.Mapping(unknown='preserve')

    def flatten(self, nested):
        return flatten_dict(nested)

    def unflatten(self, flatten):
        return unflatten_dict(flatten)

    def deserialize(self, cstruct):
        return super().deserialize({k: v for k, v in cstruct.items()
                                    if v is not None})

    def serialize(self, appstruct):
        return super().serialize({k: v for k, v in appstruct.items()
                                  if v is not None})


class InferredObjectSchema(BaseMappingSchema):
    id = colander.SchemaNode(colander.String(),
                             missing=colander.drop)
    last_modified = colander.SchemaNode(colander.DateTime(default_tzinfo=None),
                                        missing=colander.drop)

    def _bind(self, kw):
        descriptor = kw.pop('descriptor', None)

        if descriptor is None:
            return super()._bind(kw)

        fields = {k: TypeField().as_schema_node(field)
                  for k, field in descriptor.properties.items()}

        for k, v in fields.items():
            self[k] = v

        return super()._bind(kw)


class TypeField(colander.MappingSchema):
    type = colander.SchemaNode(colander.String(),
                               required=True)
    format = colander.SchemaNode(colander.String(),
                                 missing='',
                                 default='')
    primary_key = colander.SchemaNode(colander.Boolean(),
                                      missing=False,
                                      default=False)
    partition_key = colander.SchemaNode(colander.Boolean(),
                                        missing=False,
                                        default=False)
    index = colander.SchemaNode(colander.Boolean(),
                                missing=False,
                                default=False)
    required = colander.SchemaNode(colander.Boolean(),
                                   missing=False,
                                   default=False)

    def schema_type(self):
        return colander.Mapping(unknown='ignore')

    def as_schema_node(self, appstruct):
        type_, format_ = appstruct.type, appstruct.format or None
        node = JSONSCHEMA_COLANDER_TYPE_MAPPER.get((type_, format_))
        missing = colander.required if appstruct.required else colander.drop
        return colander.SchemaNode(node(), missing=missing)

    def serialize(self, appstruct):
        return {k: v for k, v in appstruct.items() if v}

    def deserialize(self, cstruct):
        return DescriptorFieldType(**super().deserialize(cstruct))


class TypeProperties(colander.MappingSchema):
    def schema_type(self):
        return colander.Mapping(unknown='raise')

    def deserialize(self, cstruct):
        if cstruct == colander.null:
            return cstruct
        return {k: TypeField().deserialize(v) for k, v in cstruct.items()}

    def serialize(self, appstruct):
        if appstruct is None:
            return
        return {k: TypeField().serialize(v) for k, v in appstruct.items()}


class InferredTypeSchema(InferredObjectSchema):
    properties = colander.SchemaNode(TypeProperties,
                                     default={})

    def deserialize(self, cstruct):
        properties = cstruct.get('properties', {})
        return super().deserialize({
            'properties': TypeProperties.deserialize(self, properties),
            **{k: v for k, v in cstruct.items() if k != 'properties'}  # noqa
        })

    def serialize(self, appstruct):
        properties = appstruct.get('properties', {})
        return super().serialize({
            'properties': TypeProperties.serialize(self, properties),
            **{k: v for k, v in appstruct.items() if k != 'properties'}  # noqa
        })

    def flatten(self, appstruct):
        properties = appstruct.pop('properties')
        return {
            'properties': properties,
            **super().flatten(appstruct)
        }


class UserSchema(InferredObjectSchema):
    def serialize(self, appstruct):
        return super().serialize({
            k: v for k, v in appstruct.items() if k != '_password'
        })


class PermissionSchema(BaseMappingSchema):
    id = colander.String()
    read = colander.Boolean()
    write = colander.Boolean()
    create = colander.Boolean()

    def deserialize(self, cstruct):
        deserialized = super().deserialize(cstruct)
        deserialized['create_'] = deserialized.pop('create', False)
        return deserialized

    def serialize(self, appstruct):
        appstruct['create'] = appstruct.pop('create_', False)
        return super().serialize(appstruct)


class BatchRequestSchema(colander.MappingSchema):
    method = colander.SchemaNode(colander.String(),
                                 validator=valid_http_method,
                                 missing=colander.drop)
    path = colander.SchemaNode(colander.String(),
                               validator=colander.Regex('^/'))
    headers = colander.SchemaNode(colander.Mapping(unknown='preserve'),
                                  validator=_check_string_values,
                                  missing=colander.drop)
    body = colander.SchemaNode(colander.Mapping(unknown='preserve'),
                               missing=colander.drop)

    @staticmethod
    def schema_type():
        return colander.Mapping(unknown='raise')


class BatchPayloadSchema(colander.MappingSchema):
    defaults = BatchRequestSchema(missing=colander.drop).clone()
    requests = colander.SchemaNode(colander.Sequence(),
                                   BatchRequestSchema())

    @staticmethod
    def schema_type():
        return colander.Mapping(unknown='raise')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # On defaults, path is not mandatory.
        self.get('defaults').get('path').missing = colander.drop

    def deserialize(self, cstruct=colander.null):
        """Preprocess received data to carefully merge defaults."""

        if cstruct is not colander.null:
            defaults = cstruct.get('defaults')
            requests = cstruct.get('requests')
            if isinstance(defaults, dict) and isinstance(requests, list):
                for request in requests:
                    if isinstance(request, dict):
                        merge_dicts(request, defaults)
        return super().deserialize(cstruct)


class BatchRequest(colander.MappingSchema):
    body = BatchPayloadSchema()


def monkey_patch_colander():
    # Recover boolean values which were coerced into strings.
    serialize_boolean = getattr(colander.Boolean, 'serialize')

    def patched_boolean_serialization(*args, **kwds):
        result = serialize_boolean(*args, **kwds)
        if result is not colander.null:
            result = result == 'true'
        return result
    setattr(colander.Boolean, 'serialize', patched_boolean_serialization)

    # Recover float values which were coerced into strings.
    serialize_float = getattr(colander.Float, 'serialize')

    def patched_float_serialization(*args, **kwds):
        result = serialize_float(*args, **kwds)
        if result is not colander.null:
            result = float(result)
        return result
    setattr(colander.Float, 'serialize', patched_float_serialization)

    # Recover integer values which were coerced into strings.
    serialize_int = getattr(colander.Int, 'serialize')

    def patched_int_serialization(*args, **kwds):
        result = serialize_int(*args, **kwds)
        if result is not colander.null:
            result = int(result)
        return result
    setattr(colander.Int, 'serialize', patched_int_serialization)

    # Remove optional mapping keys which were associated with 'colander.null'.
    serialize_mapping = getattr(colander.MappingSchema, 'serialize')

    def patched_mapping_serialization(*args, **kwds):
        result = serialize_mapping(*args, **kwds)
        if result is not colander.null:
            result = {k: v for k, v in result.items()
                      if v is not colander.null}
        return result
    setattr(colander.MappingSchema, 'serialize', patched_mapping_serialization)


monkey_patch_colander()
