# -*- coding: utf-8 -*-
import copy

from datetime import date
from uuid import uuid1
from marshmallow import Schema, fields, pprint


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


class BaseBlock(object):
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
            "params": [
                {"name": "fold number", "type": "int", "default": 10, "value": None},

            ],
            "input_ports": [
                {"name": "mRNA", "type": "ExpressionSet",
                 "bound_to": None},
                {"name": "miRNA", "type": "ExpressionSet",
                 "bound_to": {
                     "block_uuid": "123541",
                     "var_name": "es"
                 }}
            ],
        }


    }

    def __init__(self, exp_id, owner_id, uuid):
        self._doc = {
            "exp_id": exp_id,
            "owner_id": owner_id,
            "uuid": uuid,

            "is_meta_block": False,

            "hidden": {
                "is_abstract": False,
                "support_auto_exec": True,
            },

            "ui": {
                "folded": False,
            }
        }

    @property
    def uuid(self):
        return self._doc["uuid"]

    def set_ui(self, key, value):
        if "ui" not in self._doc:
            self._doc["ui"] = {}

        self._doc["ui"][key] = value

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


    @classmethod
    def new(cls, exp_id, owner_id):
        uuid = "B" + uuid1().hex[:8]
        block = cls(exp_id, owner_id, uuid)
        return block


class ClassifierBlock(BaseBlock):
    def __init__(self, *args, **kwargs):
        super(ClassifierBlock, self).__init__(*args, **kwargs)

        self.set_ui("folded", False)