# -*- coding: utf-8 -*-
import abc
import copy

import time
from uuid import uuid1

from marshmallow import Schema, fields

from webapp.tasks import wrapper_user_action
from workflow.blocks.fields import ParamField, InputType, FieldType, BlockField


class BlockAction(object):

    def __init__(self, action_name, source_states,
                 user_title=None, destination_states=None):
        """

        :param action_name:
        :param source_states:
        :param user_title:
        :param destination_states: Only informal usage
        :return:
        """

        self.action_name = action_name
        self.source_states = source_states
        self.user_title = user_title
        self.destination_states = destination_states or []

    def __str__(self):
        return "<BlockAction: [{}]--{}-->[{}]>".format(
            self.source_states,
            self.user_title or self.action_name,
            self.destination_states
        )


class ExecAction(object):

    def __init__(self, action_name, cb_on_success, cb_on_error):
        self.action_name = action_name
        self.cb_on_success = cb_on_success
        self.cb_on_error = cb_on_error

    def __str__(self):
        return "<ExecAction: {}-->{} on error {} >".format(
            self.action_name,
            self.cb_on_success,
            self.cb_on_error,
        )


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


class UserFsmStates(object):

    MODIFIED = "modified"
    RUNNING = "running"
    READY = "ready"


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


class ScopeSchema(Schema):
    exp_id = fields.Int(required=True)
    name = fields.Str(required=True)

    et_list = fields.List(fields.Str())
    latest_et = fields.Str()

    def make_object(self, data):
        return Scope(**data)


class Scope(object):
    _COLLECTION_NAME = "scopes"

    def __init__(self, exp_id, name, **kwargs):
        self.exp_id = exp_id
        self.name = name

        self.version = None  # Not implemented

        self.et_list = kwargs.get("et_list", set())
        self.latest_et = kwargs.get("latest_et")

    def __str__(self):
        return str(ScopeSchema().dump(self).data)

    def next_et(self):
        if self.latest_et is None:
            self.latest_et = 0
        else:
            self.latest_et += 1
        self.et_list.add(self.latest_et)

        return self.latest_et

    def remove_et(self, et):
        if et == self.latest_et:
            raise RuntimeError("Couldn't remove active et")
        if et in self.latest_et:
            self.et_list.remove(et)

    def _serialize_to_mongo(self):
        return ScopeSchema().dump(self)

    @classmethod
    def _deserialize_from_mongo(cls, raw):
        return ScopeSchema().load(raw).data

    @classmethod
    def load(cls, conn_db, exp_id, name, version=None):
        """
        Load scope instance from mongo
        """
        print("load invoked")
        coll = conn_db[cls._COLLECTION_NAME]
        raw = coll.find_one({
            "exp_id": exp_id,
            "name": name,
        })

        scope = cls._deserialize_from_mongo(raw)
        print("load done")
        return scope

    def save(self, conn_db):
        print("save invoked")
        coll = conn_db[self._COLLECTION_NAME]

        coll.insert(self._serialize_to_mongo())
        print("save done")


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

        # collect fsm actions
        user_fsm = attrs.get("_user_fsm", [])
        exec_fsm = attrs.get("_exec_fsm", [])
        for base in bases:
            user_fsm.extend(getattr(base, "_user_fsm", []))
            exec_fsm.extend(getattr(base, "_exec_fsm", []))

        setattr(new_class, "_user_fsm", user_fsm)
        setattr(new_class, "_exec_fsm", exec_fsm)

        return new_class

class BaseBlock(object):
    __metaclass__ = BlockMeta
    _init_fields = []
    _COLLECTION_NAME = "blocks"

    __proto_doc__ = {
        "_id": "mongo document PK",
        "uuid": "Unique block id",
        # "version": "version for history ", # TODO: implement later
        "exp_id": "django ORM experiment pk",
        "owner_id": "django ORM experiment owner pk",

        "scope": "name of scope",  # or reference to explicit object ?
        "parent_scope": "name of parent scope, None for root level blocks",  # or reference to explicit object ?
        "sub_scope": "name of sub scope",  # Optional ! only for metablocks,

        "celery_user_task_id": None,

        "is_meta_block": False,

        # "hidden": {
        #     "is_abstract": False,
        #     "support_auto_exec": True,
        # },

        "_user_fsm_state": "modified",
        # state and variables affected by Execution Token
        "et": {
            "0001": {
                "etoken": "0001",
                "parent_etoken": "0000",  # for easy discover
                "celery_task_id": None,  # when async task issued remember celery task id
                # for block itself
                "local_vars": {
                    "foo": 2.123,
                    "bar": "BLOBSTRING"
                },

                # to access from external blocks
                "output_vars": {
                    "model": {
                        "type": "",
                        "value": "",
                    },
                    "stats": {},
                },

                # to access from blocks in sub scope
                "inner_output_vars": {
                    # sub-scope ET -> variable collection
                    "0001-0000": {
                        "es_train": "<Expression Set Fold 1 train part>",
                        "es_test": "<Expression Set Fold 1 test part>"
                    },
                },
                "_exec_fsm_state": "notready"
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
                     "type": "int", "default": 10,
                     "value": None},

            },
            "input_ports": {
                "mRNA": {"name": "mRNA", "type": "ExpressionSet", "bound_to": None},
                "miRNA": {
                    "name": "miRNA", "type": "ExpressionSet",
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
    exp_id = BlockField(field_type=FieldType.STR, init_val=None, is_immutable=True)

    celery_user_task_id = BlockField(field_type=FieldType.HIDDEN, init_val=None)

    _summary = BlockField(name="summary", field_type=FieldType.STR, is_a_property=True)

    errors = BlockField(field_type=FieldType.SIMPLE_LIST, init_val=[])

    scope = BlockField(field_type=FieldType.STR)
    is_meta_block = BlockField(field_type=FieldType.BOOLEAN, init_val=False, is_immutable=True)

    _user_fsm_state = BlockField(field_type=FieldType.STR)
    block_group = "Common"
    _block_group = BlockField(name="block_group", is_a_property=True, field_type=FieldType.STR)

    _user_fsm = []
    _user_fsm_dict = None
    _exec_fsm = []
    _exec_fsm_dict = None
    # _exec_fsm = [
    #     ExecAction("run", "on_done", "on_exec_error"),  # dummy execution action
    # ]

    def __init__(self, exp_id, owner_id, uuid):
        self._doc = {
            "exp_id": exp_id,
            "owner_id": owner_id,
            "uuid": uuid,
            "hidden": {},
            "next_action": None,


            # setting for web presentation
            "ui": {},

            # descriptive info
            "configuration": {
                "fields": {},
                "params": {},
                "input_ports": {},

                "local_vars": {},
                "output_vars": {},
                "inner_output_vars": {},
            },

            "_user_fsm_state": UserFsmStates.MODIFIED,
            "et": {}
        }

        # strictly runtime variable
        self._db_provider = None

    def register_db_provider(self, db_provider):
        """
        :param db_provider: Object with method .get_db()
        """
        self._db_provider = db_provider

    def get_db_conn(self):
        if not self._db_provider:
            raise RuntimeError("No db provider set")
        else:
            return self._db_provider.get_db()

    def add_et_section(self, etoken, parent_etoken=None):
        base = {
            "etoken": etoken,
            "parent_etoken": parent_etoken,

            "_exec_fsm_state": "initiated",

        }
        self._doc["et"][str(etoken)] = base

    @classmethod
    def get_user_action_by_name(cls, name):
        """

        :rtype: BlockAction
        """
        if cls._user_fsm_dict is None:
            cls._user_fsm_dict = {
                ba.action_name: ba
                for ba in cls._user_fsm
            }

        return cls._user_fsm_dict[name]

    @classmethod
    def get_exec_action_by_name(cls, name):
        """

        :rtype: ExecAction
        """
        if cls._exec_fsm_dict is None:
            cls._exec_fsm_dict = {
                ea.action_name: ea
                for ea in cls._exec_fsm
            }

        return cls._exec_fsm_dict[name]


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
        # TODO: add method to update only SOME fields
        print("save invoked")
        coll = conn_db[self._COLLECTION_NAME]

        #
        # if "_id" in self._doc:
        #     coll.update(spec={"_id": bson.ObjectId(self._doc["_id"])},
        #                 document=self._serialize_to_mongo(),
        #                 upsert=True)
        # else:
        #     coll.insert(document=self._serialize_to_mongo())
        #
        to_save = self._serialize_to_mongo()

        coll.save(to_save)
        print("save done")

    @classmethod
    def load(cls, conn_db, exp_id, uuid, version=None):
        """
        Load block instance from mongo

        :param conn_db:
        :param exp_id:
        :param uuid:
        :param version: <Not Implemented>
        :return: Block instance
        """
        print("load invoked")
        coll = conn_db[cls._COLLECTION_NAME]
        raw = coll.find_one({
            "exp_id": exp_id,
            "uuid": uuid,
        })
        block = cls(exp_id, raw["owner_id"], raw["uuid"])
        block._deserialize_from_mongo(raw)
        print("load done")
        return block

    def add_error(self, error):
        self._doc["errors"].append(error)

    def set_block_field(self, field):
        self._doc["configuration"]["fields"][field.name] = field.to_dict()
        if not field.is_a_property and field.name not in self._doc:
            self._doc[field.name] = field.init_val

    def set_param_field(self, field):
        self._doc["configuration"]["params"][field.name] = {
            "field": field.to_dict(),  # TODO: marshmallow ! again
            "value": getattr(field, "init_val", None),
        }

    def set_user_fsm_state(self, state):
        self._doc["_user_fsm_state"] = state

    def get_user_fsm_state(self):
        return self._doc["_user_fsm_state"]

    def __getattr__(self, name):
        if name in self._doc["configuration"]["fields"]:
            return self._doc[name]
        else:
            raise AttributeError("{} object has no attribute {}"
                                 .format(self.__class__, name))

    def get_param_value(self, name):
        return self._doc["configuration"]["params"][name]["value"]

    def set_param_value(self, name, value):
        self._doc["configuration"]["params"][name]["value"] = value

    def set_hidden_value(self, field):
        self._doc["hidden"][field.name] = field.init_val

    @classmethod
    def new(cls, exp_id, owner_id, scope):
        uuid = "B" + uuid1().hex[:8]
        block = cls(exp_id, owner_id, uuid)
        block._doc["class_name"] = cls.__name__
        block._doc["scope"] = scope.name

        for field in cls._init_fields:
            if isinstance(field, ParamField):
                block.set_param_field(field)
            # elif isinstance(field, HiddenValueField):
            #     block.set_hidden_value(field)
            elif isinstance(field, BlockField):
                block.set_block_field(field)

        block.add_et_section(scope.latest_et)  # TODO: fetch parent scope ET
        return block

    def auto_execute(self, et, conn_db):
        """
        Starts auto execution flow
        :param et:
        :param conn_db:
        :return:
        """
        pass

    def run(self, et, conn_db):
        """
        Reserved action to initiate execution. Override it.
        :param et:
        :return:
        """
        self._doc["_user_fsm_state"] = UserFsmStates.RUNNING
        # self.save(conn_db)

    def on_exec_error(self, et):
        self._doc["_user_fsm_state"] = UserFsmStates.MODIFIED

    def on_done(self, et, conn_db):
        """
        Reserved action to finalize block execution. Override it.
        :param et:
        :return:
        """
        self._doc["_user_fsm_state"] = UserFsmStates.MODIFIED
        # self.save(conn_db)

    def do_user_action(self, conn_db, action_name):
        """
        :param action_name:
        :return:
        """

        ba = self.get_user_action_by_name(action_name)
        current_state = self._user_fsm_state
        print("CURRENT STATE: {}".format(current_state))
        if current_state not in ba.source_states:
            raise RuntimeError("Trying to apply action: {} but current state is: {}".format(
                action_name, current_state
            ))

        getattr(self, action_name)()

        #self.save(conn_db)

    def start_async_user_action(self, action_name):
        conn_db = self.get_db_conn()
        # ba = self.get_user_action_by_name(action_name)
        # current_state = self._user_fsm_state
        # print("CURRENT STATE: {}".format(current_state))
        # if current_state not in ba.source_states:
        #     raise RuntimeError("Trying to apply action: {} but current state is: {}".format(
        #         action_name, current_state
        #     ))
        task_id = uuid1().hex
        celery_task = wrapper_user_action.s(self.__class__, self.exp_id, self.uuid, action_name)
        self.celery_user_task_id = task_id
        self.save(conn_db)
        celery_task.apply_async(task_id=task_id)

    def do_auto_exec_action(self, conn_db, etoken):
        pass


class GenericMetaBlock(BaseBlock):
    is_meta_block = BlockField(field_type=FieldType.BOOLEAN, init_val=True, is_immutable=True)
    # summary = BlockField(field_type=FieldType.STR, init_val="Generic meta block", is_immutable=True)
    is_abstract = True


class ValidationError(object):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return "<ValidationError: {}>".format(self.msg)


class FetchGseNg(BaseBlock):
    is_abstract = False
    geo_uid = ParamField(name="geo_uid", title="Geo accession id",
                         input_type=InputType.TEXT, field_type=FieldType.STR, init_val="")

    _user_fsm = [
        BlockAction("save_changes", ["modified", "valid", "ready"],
                    user_title="Save", destination_states=["modified", "valid", "ready"]),
        BlockAction("fetch", ["valid"],
                    user_title="Fetch data", destination_states=["modified", "valid"]),
        BlockAction("preprocess", ["ready", "modified"],
                    destination_states=["valid"]),
    ]

    _exec_fsm = [
        ExecAction("run", "on_done", "on_exec_error"),  # dummy execution action
    ]

    def save_changes(self):
        # Do validation
        geo_uid = self.get_param_value("geo_uid")
        if not geo_uid.startswith("GSE"):
            self.set_user_fsm_state(UserFsmStates.MODIFIED)
            self.add_error(
                # ValidationError(msg="GEO id should start with GSE")
                dict(msg="GEO id should start with GSE")
            )
            return

        self.set_user_fsm_state("valid")

        # if <annotained>
        # elf.set_user_fsm_state(UserFsmStates.READY)

    def start_preprocess(self):
        self.start_async_user_action("_fetching")

    def _fetching(self):
        time.sleep(2)
        self.set_user_fsm_state("fetched")
        self.save(self.get_db_conn())

        # from celery.contrib import rdb; rdb.set_trace()
        self.start_async_user_action("_process")

    def _process(self):
        time.sleep(2)
        self.set_user_fsm_state("processed")
        self.save(self.get_db_conn())


class ClassifierBlock(BaseBlock):
    is_abstract = False
    pass

    # def __init__(self, *args, **kwargs):
    #     super(ClassifierBlock, self).__init__(*args, **kwargs)
        # self.set_ui("folded", False)


class CrossValidation(GenericMetaBlock):

    summary = "Cross validation"


