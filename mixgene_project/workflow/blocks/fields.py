# -*- coding: utf-8 -*-
import logging
import random
from mixgene.redis_helper import ExpKeys
from mixgene.util import get_redis_instance
import cPickle as pickle

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class MultiUploadField(dict):
    """
        Should contain UploadedFileWrapper instances
    """
    def to_dict(self, *args, **kwargs):
        return {
            "count": len(self),
            "files_info": [obj.to_dict(*args, **kwargs) for obj in self.values()],
            "previews": sorted([obj.orig_name for obj in self.values()])
        }


class FieldType(object):
    STR = "str"
    INT = "int"
    FLOAT = "float"
    BOOLEAN = "boolean"

    RAW = "raw"

    CUSTOM = "custom"  # object provides method .to_dict()

    SIMPLE_DICT = "simple_dict"
    SIMPLE_LIST = "simple_list"

    OUTPUT = "output"
    INPUT_PORT = "input_port"

    HIDDEN = "hidden"  # don't serialize this


class BlockField(object):
    _ignore_fields = set(["name", "validator", "required", "init_val", "order_num"])

    def __init__(self, name=None, field_type=FieldType.HIDDEN, init_val=None,
                 is_immutable=False, required=False, is_a_property=False,
                 order_num=None,
                 *args, **kwargs):

        self.name = name
        self.field_type = field_type
        self.init_val = init_val
        self.is_immutable = is_immutable
        self.required = required
        self.is_a_property = is_a_property

        if order_num is None:
            self.order_num = random.randint(0, 1000)
        else:
            self.order_num = order_num

    def to_dict(self):
        return {
            k: v for k, v in self.__dict__.iteritems()
            if k not in self._ignore_fields
        }

    def value_from_dict(self, dct):
        """
            Parse dict object obtained from JSON representation

        """

    def value_to_dict(self, raw_val, block):
        """
            Convert object @raw_val into the dict structure that will serialized into JSON
        """
        val = str(raw_val)
        try:
            if raw_val is None:
                if self.field_type in [FieldType.STR]:
                    val = ""
                else:
                    val = None
            else:
                if self.field_type in [FieldType.RAW, FieldType.INT,
                                       FieldType.FLOAT, FieldType.BOOLEAN]:
                    val = raw_val
                if self.field_type == FieldType.CUSTOM:
                    try:
                        val = raw_val.to_dict(block)
                    except Exception, e:
                        log.exception("Failed to serialize field %s with error: %s", self.name, e)
                        val = str(raw_val)
                if self.field_type in [FieldType.STR]:
                    val = str(raw_val)
                if self.field_type == FieldType.SIMPLE_DICT:
                    val = {str(k): str(v) for k, v in raw_val.iteritems()}
                if self.field_type == FieldType.SIMPLE_LIST:
                    val = map(str, raw_val)
            return val
        except Exception, e:
            log.exception(e)
            # TODO: fix it
            return ""

    def contribute_to_class(self, cls, name):
        getattr(cls, "_block_spec").register(self)

    def contribute_to_instance(self, owner):
        setattr(owner, self.name, self.init_val)
        # if not self.exec_token_affected:
        #     setattr(owner, self.name, self.init_val)
        # else:
        #     owner.et_field_names.add(self.name)


class HiddenValueField(BlockField):
    pass


class LocalVarField(BlockField):
    pass


class OutputBlockField(BlockField):
    def __init__(self, provided_data_type=None, *args, **kwargs):
        super(OutputBlockField, self).__init__(exec_token_affected=True, *args, **kwargs)
        self.provided_data_type = provided_data_type

    def to_dict(self):
        result = super(OutputBlockField, self).to_dict()
        result.update({
            "data_type": self.provided_data_type,
        })
        return result


class InnerOutputField(OutputBlockField):
    def __init__(self, *args, **kwargs):
        super(InnerOutputField, self).__init__(exec_token_affected=False, *args, **kwargs)


class InputType(object):
    TEXT = "text"
    SELECT = "select"
    CHOICE = "choice"
    CHECKBOX = "checkbox"
    HIDDEN = "hidden"
    FILE_INPUT = "file_input"


class ParamField(BlockField):
    _ignore_fields = set(["name", "validator", "required", "init_val",
                          "is_immutable", "is_a_property"])

    def __init__(self, title=None, input_type=InputType.TEXT,
                 validator=None, select_provider=None,
                 options=None,
                 *args, **kwargs):
        kwargs["is_a_property"] = False
        kwargs["is_immutable"] = False
        super(ParamField, self).__init__(exec_token_affected=False, *args, **kwargs)
        self.title = title
        self.input_type = input_type
        self.validator = validator
        self.select_provider = select_provider
        self.is_a_property = False
        self.options = options or {}


class InputBlockField(BlockField):
    def __init__(self, required_data_type=None, multiply_extensible=False,
                 *args, **kwargs):
        super(InputBlockField, self).__init__(exec_token_affected=True, *args, **kwargs)
        self.required_data_type = required_data_type
        self.field_type = FieldType.INPUT_PORT
        self.bound_var_key = None
        self.multiply_extensible = multiply_extensible


class ActionRecord(object):
    def __init__(self, name, src_states, dst_state, user_title=None,
                 propagate_auto_execution=False,
                 reload_block_in_client=False,
                 **kwargs):
        self.name = name
        self.src_states = src_states
        self.dst_state = dst_state
        self.user_title = user_title
        self.propagate_auto_execution = propagate_auto_execution
        self.reload_block_in_client = reload_block_in_client
        self.show_to_user = user_title is not None
        self.kwargs = kwargs


class ActionsList(object):
    def __init__(self, actions_list):
        self.actions_list = actions_list

    def extend(self, other_list):
        """
            @type other_list: ActionsList
        """
        self.actions_list.extend(other_list.actions_list)

    def contribute_to_class(self, cls, name):
        for action_record in self.actions_list:
            getattr(cls, "_trans").register(action_record)

    def to_dict(self, *args, **kwargs):
        return [{
            "code": ar.name,
            "title": ar.user_title,
        } for ar in self.actions_list]
