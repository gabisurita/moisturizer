import colander
import flatten_json

from moisturizer.models import (
    DescriptorFieldType,
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
}


class BaseMappingSchema(colander.MappingSchema):
    """Base schema to (de)serialize objects."""

    def schema_type(self):
        return colander.Mapping(unknown='preserve')

    def flatten(self, nested):
        return {k: v for k, v in
                flatten_json.flatten(nested, separator='__').items()
                if v is not None}

    def unflatten(self, flatten):
        return flatten_json.unflatten(flatten, separator='__')

    def deserialize(self, cstruct):
        id_field = cstruct.get('id')
        if id_field is not None:
            cstruct['id'] = str(id_field)

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
