# -*- coding: utf-8 -*-
import logging
import random

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
    def __init__(self, name, field_type=FieldType.HIDDEN, init_val=None,
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

    def value_to_dict(self, raw_val, block):
        val = str(raw_val)
        if raw_val is None:
            val = None
        else:
            if self.field_type in [FieldType.RAW, FieldType.INT, FieldType.FLOAT, FieldType.BOOLEAN]:
                val = raw_val
            if self.field_type == FieldType.CUSTOM:
                val = raw_val.to_dict(block)
            if self.field_type in [FieldType.STR]:
                val = str(raw_val)
            if self.field_type == FieldType.SIMPLE_DICT:
                val = {str(k): str(v) for k, v in raw_val.iteritems()}
            if self.field_type == FieldType.SIMPLE_LIST:
                val = map(str, raw_val)
        return val

    def contribute_to_class(self, cls, name):
        #setattr(cls, name, self.init_val)
        getattr(cls, "_block_serializer").register(self)


class OutputBlockField(BlockField):
    def __init__(self, provided_data_type=None, *args, **kwargs):
        super(OutputBlockField, self).__init__(*args, **kwargs)
        self.provided_data_type = provided_data_type

    def contribute_to_class(self, cls, name):
        getattr(cls, "_block_serializer").register(self)


class InnerOutputField(OutputBlockField):
    pass


class InputBlockField(BlockField):
    def __init__(self, required_data_type=None, multiply_extensible=False, options=None,
                 *args, **kwargs):
        super(InputBlockField, self).__init__(*args, **kwargs)
        self.required_data_type = required_data_type
        self.field_type = FieldType.INPUT_PORT
        self.bound_var_key = None
        self.options = options or {}
        self.multiply_extensible = multiply_extensible

    def contribute_to_class(self, cls, name):
        getattr(cls, "_block_serializer").register(self)

    def to_dict(self):
        return self.__dict__


class InputType(object):
    TEXT = "text"
    SELECT = "select"
    CHOICE = "choice"
    CHECKBOX = "checkbox"
    HIDDEN = "hidden"
    FILE_INPUT = "file_input"


class ParamField(object):
    # TODO: maybe more use of django form fields?
    # TODO: or join ParamField and BlockField?
    def __init__(self, name, title, input_type, field_type, init_val=None,
                 validator=None, select_provider=None,
                 required=True, order_num=None, options=None,
                 *args, **kwargs):
        self.name = name
        self.title = title
        self.input_type = input_type
        self.field_type = field_type
        self.init_val = init_val
        self.validator = validator
        self.select_provider = select_provider
        self.is_a_property = False
        self.required = required
        self.options = options or {}
        if order_num is None:
            self.order_num = random.randint(0, 1000)
        else:
            self.order_num = order_num

    def contribute_to_class(self, cls, name):
        #setattr(cls, name, self.init_val)
        getattr(cls, "_block_serializer").register(self)

    def to_dict(self):
        ignore_fields = set(["validator"])
        return {k: v for k, v in self.__dict__.iteritems() if k not in ignore_fields}

    def value_to_dict(self, raw_val, block):
        val = str(raw_val)
        try:
            if raw_val is None:
                if self.field_type in [FieldType.STR]:
                    val = ""
                else:
                    val = None
            else:
                if self.field_type in [FieldType.RAW, FieldType.INT, FieldType.FLOAT] :
                    val = raw_val
                if self.field_type == FieldType.CUSTOM:
                    try:
                        val = raw_val.to_dict(block)
                    except Exception, e:
                        log.exception("Failed to serialize field %s with error: %s", self.name, e)
                        val = str(raw_val)
                if self.field_type in [FieldType.STR, FieldType.BOOLEAN]:
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