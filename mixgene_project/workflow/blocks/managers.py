# -*- coding: utf-8 -*-
from collections import defaultdict
import copy
import itertools
import logging
from mixgene.util import log_timing, stopwatch
from webapp.scope import ScopeVar
from workflow.blocks.errors import PortError
from workflow.blocks.fields import InnerOutputField, OutputBlockField, InputBlockField, BlockField, ParamField, \
    FieldType

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

class TransSystem(object):
    """
        Assumptions:
            - one action can have multiple source states, but only one result state
    """
    def __init__(self):
        self.action_records_by_name = dict()
        self.states_to_actions = defaultdict(list)
        self.action_to_state = dict()
        self.is_action_visible = dict()

    @staticmethod
    def clone(other):
        """
            @type other: TransSystem
            @rtype: TransSystem
        """
        trans = TransSystem()
        trans.action_records_by_name = copy.deepcopy(other.action_records_by_name)
        trans.states_to_actions = copy.deepcopy(other.states_to_actions)
        trans.action_to_state = copy.deepcopy(other.action_to_state)
        trans.is_action_visible = copy.deepcopy(other.is_action_visible)
        return trans

    def register(self, ar):
        """
            @type ar: workflow.blocks.fields.ActionRecord
        """
        self.action_records_by_name[ar.name] = ar
        for state in ar.src_states:
            self.states_to_actions[state].append(ar.name)
        self.action_to_state[ar.name] = ar.dst_state
        self.is_action_visible[ar.name] = ar.show_to_user

    def user_visible(self, state):
        return set([
            self.action_records_by_name[action]
            for action in itertools.chain(
                self.states_to_actions.get(state, []),
                self.states_to_actions.get("*", [])
            )
            if self.is_action_visible[action]
        ])

    def is_action_available(self, state, action_name):
        if action_name not in self.action_records_by_name:
            return False
        if action_name in self.states_to_actions[state] or \
                action_name in self.states_to_actions["*"]:
            return True
        else:
            return False

    def next_state(self, state, action_name):
        if action_name not in self.action_records_by_name:
            return None

        if action_name in self.states_to_actions[state] or \
                action_name in self.states_to_actions["*"]:
            return self.action_to_state[action_name]
        else:
            return None


class BlockSerializer(object):
    def __init__(self):
        self.fields = dict()
        self.params = dict()
        self.outputs = dict()
        self.inner_outputs = dict()
        self.inputs = dict()

    def register(self, field):
        if isinstance(field, InnerOutputField):
            self.inner_outputs[field.name] = field
            return

        if isinstance(field, OutputBlockField):
            self.outputs[field.name] = field
            return

        if isinstance(field, InputBlockField):
            self.inputs[field.name] = field
            return

        if isinstance(field, BlockField):
            self.fields[field.name] = field
            return

        if isinstance(field, ParamField):
            self.params[field.name] = field
            return

    @staticmethod
    def clone(other):
        bs = BlockSerializer()
        bs.fields = copy.deepcopy(other.fields)
        bs.params = copy.deepcopy(other.params)
        bs.outputs = copy.deepcopy(other.outputs)
        bs.inner_outputs = copy.deepcopy(other.inner_outputs)
        bs.inputs = copy.deepcopy(other.inputs)
        return bs

    @log_timing
    def to_dict(self, block):
        result = {}
        for f_name, f in self.fields.iteritems():
            if f.field_type == FieldType.HIDDEN:
                continue

            with stopwatch(name="Serializing block field %s" % f_name, threshold=0.01):
                raw_val = getattr(block, f_name)
                result[f_name] = f.value_to_dict(raw_val, block)

        params_protype = {
            str(param_name): param_field.to_dict()
            for param_name, param_field in self.params.iteritems()
        }
        result["_params_prototype"] = params_protype
        result["_params_prototype_list"] = params_protype.values()

        for f_name, f in self.params.iteritems():
            with stopwatch(name="Serializing block param %s" % f_name, threshold=0.01):
                raw_val = getattr(block, f.name)
                result[f_name] = f.value_to_dict(raw_val, block)

        result["actions"] = [{
            "code": ar.name,
            "title": ar.user_title,
        } for ar in block.get_user_actions()]

        result["out"] = block.out_manager.to_dict(block)
        result["inputs"] = block.input_manager.to_dict()

        return result

    def validate_params(self, block, exp):
        is_valid = True
        for p_name, p in self.params.iteritems():
            if p.validator is not None and hasattr(p.validator, '__call__'):
                val = getattr(block, p.name)
                try:
                    p.validator(val, block)
                except Exception, e:
                    is_valid = False
                    block.error(exp, e)

        return is_valid

    def save_params(self, block, received_block):
        """
            @param block: GenericBlock
            @param received_block: dict
        """
        for p_name, p in self.params.iteritems():
            # TODO: here invoke validator
            raw_val = received_block.get(p_name)

            # TODO: add conversion to BlockField class
            if p.field_type == FieldType.FLOAT:
                val = float(raw_val)
            elif p.field_type == FieldType.INT:
                val = int(raw_val)
            elif p.field_type == FieldType.STR:
                val = str(raw_val)
            elif p.field_type == FieldType.RAW:
                val = raw_val
            else:
                continue
                #val = raw_val

            setattr(block, p_name, val)

        inputs_dict = received_block.get('bound_inputs')
        if inputs_dict:
            for _, input_field in self.inputs.iteritems():
                key = inputs_dict.get(input_field.name)
                if key:
                    var = ScopeVar.from_key(key)
                    block.bind_input_var(input_field.name, var)


class OutManager(object):
    def __init__(self):
        self.data_type_by_name = {}
        self.fields_by_data_type = defaultdict(list)

    def contains(self, var_name):
        if var_name in self.data_type_by_name:
            return True
        else:
            return False

    def register(self, name, data_type):
        if name in self.data_type_by_name.keys():
            raise KeyError("Field with name %s already exists" % name)

        self.data_type_by_name[name] = data_type
        self.fields_by_data_type[data_type].append(name)

    def get_fields_by_data_type(self, data_type):
        return self.fields_by_data_type[data_type]

    def to_dict(self, block):
        result = {}
        for fname, _ in self.data_type_by_name.iteritems():
            var = block.get_out_var(fname)
            if var and hasattr(var, "to_dict"):
                result[fname] = var.to_dict()

        return result


class InputManager(object):
    def __init__(self):
        self.input_fields = []

    def register(self, input_field):
        self.input_fields.append(input_field)

    def validate_inputs(self, block, bound_inputs, errors, warnings):
        is_valid = True
        for f in self.input_fields:
            if f.multiply_extensible:
                continue
            if bound_inputs.get(f.name) is None:
                exception = PortError(block, f.name, "Input port not bound")
                if f.required:
                    is_valid = False
                    errors.append(exception)
                else:
                    warnings.append(exception)
        return is_valid

    def to_dict(self):
        return {
            "list": [
                field.to_dict()
                for field in self.input_fields
            ]
        }



class IteratedInnerFieldManager(object):
    def __init__(self):
        self.fields = {}
        self.sequence = []
        self.iterator = -1

    def register(self, field):
        """
            @type field: workflow.blocks.fields.BlockField
        """
        self.fields[field.name] = field

    def next(self):
        self.iterator += 1
        log.debug("Iterated inner field shifted iterator to pos %s from %s",
              self.iterator, len(self.sequence))
        if self.iterator >= len(self.sequence):
            raise StopIteration()

    def reset(self):
        self.sequence = []
        self.iterator = -1

    def get_var(self, fname):
        if self.iterator < 0:
            return RuntimeError("Iteration wasn't started")
        elif self.iterator >= len(self.sequence):
            return StopIteration()
        else:
            return self.sequence[self.iterator][fname]