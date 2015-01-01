# -*- coding: utf-8 -*-
import abc
import copy

from datetime import date
from uuid import uuid1
from marshmallow import Schema, fields, pprint
from workflow.blocks.fields import ParamField, InputType, FieldType, BlockField, HiddenValueField


def get_schema_class(spec, base_klass=None):
    """

    :param spec: Specification of fields
    [
        (field name, type, *args, **kwargs)
    ]
    :return:
    """
    if not base_klass:
        class Base(object):
            pass
    else:
        Base = base_klass

    for name, field_type, args, kwargs in spec:
        setattr(Base, name, field_type(*args, **kwargs))

    class CustomSchema(Base, Schema):
        pass

    return CustomSchema




class ETokenizedBaseSchema(object):
    etoken = fields.Str()


class BaseBlockSchema(object):
    uuid = fields.Str()
    exp_id = fields.Int(required=True)

    scope = fields.Str()
    parent_scope = fields.Str()


class ExperimentDummy(object):
    __proto_doc__ = {
        "_id": "django ORM PK",
        "owner": "reference to django user",
        # "version": "version for history",  #  TODO
    }
    pass


BLOCKS_COLLECTION_NAME = "blocks"

taboo_attrs = [
    "__metaclass__",
    "is_abstract",
    "_trans",
    "_block_spec",
    "exec_token",

    "_init_params",
]


class BlockMeta(abc.ABCMeta):
    def __new__(cls, name, bases, attrs):
        super_new = super(BlockMeta, cls).__new__
        module = attrs.pop('__module__')
        new_class = super_new(cls, name, bases, {'__module__': module})

        # collect block initial parameters
        init_fields = []
        for attr_name, attr_value in attrs.iteritems():
            if isinstance(attr_value, BlockField):
                if getattr(attr_value, "name") is None:
                    setattr(attr_value, "name", attr_name)
                init_fields.append(attr_value)
            else:
                setattr(new_class, attr_name, attr_value)

        for base in bases:
            init_fields.extend(getattr(base, "_init_fields", []))

        setattr(new_class, "_init_fields", init_fields)
        return new_class


class BaseBlock(object):

    __metaclass__ = BlockMeta

    _init_fields = []

    __proto_doc__ = {
        "_id": "mongo document PK",
        "uuid": "Unique block id",
        # "version": "version for history ", # TODO: implement later
        "exp_id": "django ORM experiment pk",
        "owner_id": "django ORM experiment owner pk",
        "scope": "name of scope",  # or reference to explicit object ?
        "parent_scope": "name of parent scope, None for root level blocks",  # or reference to explicit object ?
        "sub_scope": "name of sub scope",  # Optional ! only for metablocks,

        "is_meta_block": False,

        "hidden": {
            "is_abstract": False,
            "support_auto_exec": True,
        },

        "etokenized": {
            "0001": {
                "_id": "0001",
                "parent_et": "0000",  # for easy discover
                "local_vars": {
                    "foo": 2.123,
                    "bar": "BLOBSTRING"
                },
                "fsm_state": "initiated"
            }
        },

        "ui": {
            "folded": True,
            "show_collector_editor": False,
        },

        "errors": [],
        "warnings": [],

        "configuration": {
            "fields": {
                "state": {
                    "is_property": True
                }
            },
            "params": {
                "fold number":
                    {"name": "fold number",
                     "type": "int", "default": 10, "value": None},

            },
            "input_ports": {
                "mRNA": {"name": "mRNA", "type": "ExpressionSet", "bound_to": None},
                "miRNA": {"name": "miRNA", "type": "ExpressionSet",
                          "bound_to": {
                              "block_uuid": "123541",
                              "var_name": "es"
                          }
                }
            },
        }


    }
    available_user_actions = BlockField(is_a_property=True, field_type=FieldType.CUSTOM)
    uuid = BlockField(field_type=FieldType.STR, init_val=None, is_immutable=True)

    scope = BlockField(field_type=FieldType.STR)

    def __init__(self, exp_id, owner_id, uuid):
        self._doc = {
            "exp_id": exp_id,
            "owner_id": owner_id,
            "uuid": uuid,
            "hidden": {},

            "ui": {},

            "configuration": {
                "fields": {},
                "params": {},
                "input_ports": {},
            },
        }

    # def set_ui(self, key, value):
    #     if "ui" not in self._doc:
    #         self._doc["ui"] = {}
    #
    #     self._doc["ui"][key] = value

    def _serialize_to_mongo(self):
        # TODO: use marshmarshallow
        return copy.deepcopy(self._doc)

    def _deserialize_from_mongo(self, raw):
        # TODO: use marshmarshallow
        self._doc = copy.deepcopy(raw)

    def save(self, conn_db):
        print("save invoked")
        coll = conn_db[BLOCKS_COLLECTION_NAME]

        coll.insert(self._serialize_to_mongo())
        print("save done")

    @classmethod
    def load(cls, conn_db, exp_id, uuid, version=None):
        """

        :param conn_db:
        :param exp_id:
        :param uuid:
        :param version: Not Implemented
        :return: Block instance
        """
        print("load invoked")
        coll = conn_db[BLOCKS_COLLECTION_NAME]
        raw = coll.find_one({
            "exp_id": exp_id,
            "uuid": uuid,
        })
        block = cls(exp_id, raw["owner_id"], raw["uuid"])
        block._deserialize_from_mongo(raw)
        print("load done")
        return block

    def set_block_field(self, field):
        self._doc["configuration"]["fields"][field.name] = field.to_dict()
        if not field.is_a_property and field.name not in self._doc:
            self._doc[field.name] = field.init_val

    def set_param_field(self, field):
        self._doc["configuration"]["params"][field.name] = {
            "field": field.to_dict(),  # TODO: marshmallow ! again
            "value": getattr(field, "init_val", None),
        }

    def __getattr__(self, name):
        if name in self._doc["configuration"]["fields"]:
            return self._doc[name]
        else:
            raise AttributeError("{} object has no attribute {}"
                                 .format(self.__class__, name))

    def get_param_value(self, name):
        return self._doc["configuration"]["params"][name]

    def set_param_value(self, name, value):
        self._doc["configuration"]["params"][name] = value

    def set_hidden_value(self, field):
        self._doc["hidden"][field.name] = field.init_val

    @classmethod
    def new(cls, exp_id, owner_id):
        uuid = "B" + uuid1().hex[:8]
        block = cls(exp_id, owner_id, uuid)

        for field in cls._init_fields:
            if isinstance(field, ParamField):
                block.set_param_field(field)
            elif isinstance(field, HiddenValueField):
                block.set_hidden_value(field)
            elif isinstance(field, BlockField):
                block.set_block_field(field)

        return block


class FetchGseNg(BaseBlock):
    # is_abstract = HiddenValueField(init_val=False)
    geo_uid = ParamField(name="geo_uid", title="Geo accession id",
                         input_type=InputType.TEXT, field_type=FieldType.STR, init_val="")


class ClassifierBlock(BaseBlock):
    pass

    # def __init__(self, *args, **kwargs):
    #     super(ClassifierBlock, self).__init__(*args, **kwargs)

        # self.set_ui("folded", False)