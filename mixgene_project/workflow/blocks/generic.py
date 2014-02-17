from pprint import pprint
import collections
import copy
import itertools
from webapp.models import Experiment
from webapp.scope import Scope, ScopeVar
from webapp.scope import auto_exec_task
from workflow.execution import ExecStatus

from uuid import uuid1
from workflow.ports import BoundVar



from collections import defaultdict
from uuid import uuid1
from webapp.scope import ScopeRunner

class ActionRecord(object):
    def __init__(self, name, src_states, dst_state, user_title=None,**kwargs):
        self.name = name
        self.src_states = src_states
        self.dst_state = dst_state
        self.user_title = user_title
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
            @type ar: ActionRecord
        """
        self.action_records_by_name[ar.name] = ar
        for state in ar.src_states:
            self.states_to_actions[state].append(ar.name)
        self.action_to_state[ar.name] = ar.dst_state
        self.is_action_visible[ar.name] = ar.show_to_user

    def user_visible(self, state):
        return [
            self.action_records_by_name[action] for action in self.states_to_actions.get(state, [])
            if self.is_action_visible[action]
        ]

    def is_action_available(self, state, action_name):
        if action_name not in self.action_records_by_name:
            return False
        if action_name in self.states_to_actions[state]:
            return True
        else:
            return False

    def next_state(self, state, action_name):
        if action_name not in self.action_records_by_name:
            return None

        if action_name in self.states_to_actions[state]:
            return self.action_to_state[action_name]
        else:
            return None


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
                 *args, **kwargs):
        self.name = name
        self.field_type = field_type
        self.init_val = init_val
        self.is_immutable = is_immutable
        self.required = required
        self.is_a_property = is_a_property

    def value_to_dict(self, raw_val, block):
        val = str(raw_val)
        if raw_val is None:
            val = None
        else:
            if self.field_type in [FieldType.RAW, FieldType.INT, FieldType.FLOAT] :
                val = raw_val
            if self.field_type == FieldType.CUSTOM:
                val = raw_val.to_dict(block)
            if self.field_type in [FieldType.STR, FieldType.BOOLEAN]:
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
    def __init__(self, required_data_type=None, *args, **kwargs):
        super(InputBlockField, self).__init__(*args, **kwargs)
        self.required_data_type = required_data_type
        self.field_type = FieldType.INPUT_PORT
        self.bound_var_key = None

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



# TODO: maybe more use of django form fields?
# TODO: or join ParamField and BlockField?
class ParamField(object):
    def __init__(self, name, title, input_type, field_type, init_val=None,
                 validator=None, select_provider=None, *args, **kwargs):
        self.name = name
        self.title = title
        self.input_type = input_type
        self.field_type = field_type
        self.init_val = init_val
        self.validator = validator
        self.select_provider = select_provider
        self.is_a_property = False

    def contribute_to_class(self, cls, name):
        #setattr(cls, name, self.init_val)
        getattr(cls, "_block_serializer").register(self)

    def to_dict(self):
        return {k: str(v) for k, v in  self.__dict__.iteritems()}

    def value_to_dict(self, raw_val, block):
        val = str(raw_val)
        if raw_val is None:
            val = None
        else:
            if self.field_type in [FieldType.RAW, FieldType.INT, FieldType.FLOAT] :
                val = raw_val
            if self.field_type == FieldType.CUSTOM:
                val = raw_val.to_dict(block)
            if self.field_type in [FieldType.STR, FieldType.BOOLEAN]:
                val = str(raw_val)
            if self.field_type == FieldType.SIMPLE_DICT:
                val = {str(k): str(v) for k, v in raw_val.iteritems()}
            if self.field_type == FieldType.SIMPLE_LIST:
                val = map(str, raw_val)
        return val


# class BoundInputs(dict):
#     def to_dict(self):
#         print list(self.iteritems())

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
        return bs

    def to_dict(self, block):
        result = {}
        for f_name, f in self.fields.iteritems():
            if f.field_type == FieldType.HIDDEN:
                continue
            raw_val = getattr(block, f_name)
            result[f_name] = f.value_to_dict(raw_val, block)

        result["_params_prototype"] = dict([(str(param_name), param_field.to_dict())
                                   for param_name, param_field in self.params.iteritems()])

        for f_name, f in self.params.iteritems():
            raw_val = getattr(block, f.name)
            result[f_name] = f.value_to_dict(raw_val, block)

        result["actions"] = [{
            "code": ar.name,
            "title": ar.user_title,
        } for ar in block.get_user_actions()]

        result["out"] = block.out_manager.to_dict(block)
        result["inputs"] = block.input_manager.to_dict()

        return result

    def save_params(self, block, received_block):
        """
            @param block: GenericBlock
            @param received_block: dict
        """
        # import  ipdb; ipdb.set_trace()
        for p_name, p in self.params.iteritems():
            # TODO: here invoke validator
            raw_val = received_block.get(p_name)

            # TODO: add conversion to BlockField class
            if p.field_type == FieldType.FLOAT:
                val = float(raw_val)
            elif p.field_type == FieldType.INT:
                val = int(raw_val)
            else:
                val = raw_val

            setattr(block, p_name, val)

        inputs_dict = received_block.get('bound_inputs')
        if inputs_dict:
            for _, input_field in self.inputs.iteritems():
                key = inputs_dict.get(input_field.name)
                if key:
                    var = ScopeVar.from_key(key)
                    block.bind_input_var(input_field.name, var)


class BlockMeta(type):
    def __new__(cls, name, bases, attrs):
        print "BlockMeta new: %s " % name
        super_new = super(BlockMeta, cls).__new__
        module = attrs.pop('__module__')
        new_class = super_new(cls, name, bases, {'__module__': module})

        if hasattr(bases[0], "_block_serializer"):
            setattr(new_class, "_block_serializer", BlockSerializer.clone(bases[0]._block_serializer))
            setattr(new_class, "_trans", TransSystem.clone(bases[0]._trans))

        for obj_name, obj in attrs.items():
            new_class.add_to_class(obj_name, obj)

        return new_class

    def add_to_class(cls, name, value):
        if hasattr(value, "contribute_to_class"):
            value.contribute_to_class(cls, name)
        else:
            setattr(cls, name, value)


class BaseBlock(object):
    _block_serializer = BlockSerializer()
    _trans = TransSystem()

    """
        WARNING: For blocks always inherit other *Block class at first position
    """
    __metaclass__ = BlockMeta


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

    def validate_inputs(self, bound_inputs, errors, warnings):
        is_valid = True
        for f in self.input_fields:
            if bound_inputs.get(f.name) is None:
                exception = Exception("Input %s hasn't bound variable")
                if f.required:
                    is_valid = False
                    errors.append(exception)
                else:
                    warnings.append(exception)
        return is_valid

    def to_dict(self):
        return {
            field.name: field.to_dict()
            for field in self.input_fields
        }


class GenericBlock(BaseBlock):
    # block fields
    uuid = BlockField("uuid", FieldType.STR, None, is_immutable=True)
    name = BlockField("name", FieldType.STR, None)
    base_name = BlockField("base_name", FieldType.STR, "", is_immutable=True)
    exp_id = BlockField("exp_id", FieldType.STR, None, is_immutable=True)

    scope_name = BlockField("scope_name", FieldType.STR, "root", is_immutable=True)
    _sub_scope_name = BlockField("sub_scope_name", FieldType.STR, None, is_immutable=True)

    state = BlockField("state", FieldType.STR, "created")

    _create_new_scope = BlockField("create_new_scope", FieldType.BOOLEAN, False)
    create_new_scope = False

    errors = BlockField("errors", FieldType.SIMPLE_LIST, list())
    warnings = BlockField("warnings", FieldType.SIMPLE_LIST, list())
    bound_inputs = BlockField("bound_inputs", FieldType.SIMPLE_DICT, defaultdict())

    def __init__(self, name, exp_id, scope_name):
        """
            Building block for workflow
        """
        self.uuid = uuid1().hex
        self.name = name
        self.exp_id = exp_id
        self.scope_name = scope_name
        self.base_name = ""

        # Used only be meta-blocks
        self.children_blocks = []
        # End


        self._out_data = dict()
        self.out_manager = OutManager()

        self.input_manager = InputManager()

        # Automatic execution status map
        self.auto_exec_status_ready = set(["ready"])
        self.auto_exec_status_done = set(["done"])
        self.auto_exec_status_working = set(["working"])
        self.auto_exec_status_error = set(["execution_error"])
        self.is_block_supports_auto_execution = False

        # Init block fields
        for f_name, f in itertools.chain(
                self._block_serializer.fields.iteritems(),
                self._block_serializer.params.iteritems()):

            #if f_name not in self.__dict__ and not f.is_a_property:
            if not hasattr(self, f_name):
                setattr(self, f_name, f.init_val)

        # TODO: Hmm maybe more metaclass magic can be applied here
        scope = self.get_scope()
        scope.load()
        for f_name, f in self._block_serializer.outputs.iteritems():
            print "Registering normal outputs: ", f_name
            self.register_provided_objects(scope, ScopeVar(self.uuid, f_name, f.provided_data_type))
        scope.store()

        if hasattr(self, "create_new_scope") and self.create_new_scope:
            print "Trying to add inner outputs"
            scope = self.get_sub_scope()
            scope.load()
            for f_name, f in self._block_serializer.inner_outputs.iteritems():
                print "Registering inner outputs: ", f_name
                scope.register_variable(ScopeVar(self.uuid, f_name, f.provided_data_type))
            scope.store()

        for f_name, f in self._block_serializer.inputs.iteritems():
            self.input_manager.register(f)

    def get_exec_status(self):
        if self.state in self.auto_exec_status_done:
            return "done"
        if self.state in self.auto_exec_status_error:
            return "error"
        if self.state in self.auto_exec_status_ready:
            return "ready"

        return "not_ready"

    def bind_input_var(self, input_name, bound_var):
        print "bound input %s to %s" % (input_name, bound_var)
        self.bound_inputs[input_name] = bound_var

    def get_input_var(self, name):
        exp = Experiment.get_exp_by_id(self.exp_id)
        scope_var = self.bound_inputs[name]
        return exp.get_scope_var_value(scope_var)

    def get_out_var(self, name):
        if self.out_manager.contains(name):
            return self._out_data.get(name)
        else:
            return self.get_inner_out_var(name)

    def get_inner_out_var(self, name):
        raise NotImplementedError("Not implemented in the base class")

    def set_out_var(self, name, value):
        self._out_data[name] = value

    def get_scope(self):
        exp = Experiment.get_exp_by_id(self.exp_id)
        return Scope(exp, self.scope_name)

    @property
    def sub_scope_name(self):
        if hasattr(self, "create_new_scope") and self.create_new_scope:
            return "%s_%s" % (self.scope_name, self.uuid)
        else:
            return ""

    def get_sub_scope(self):
        exp = Experiment.get_exp_by_id(self.exp_id)
        return Scope(exp, self.sub_scope_name)

    def reset_execution_for_sub_blocks(self):
        exp = Experiment.get_exp_by_id(self.exp_id)
        for block_uuid, block in exp.get_blocks(self.children_blocks):
            block.do_action("reset_execution", exp)

    def get_input_blocks(self):
        required_blocks = []
        for f in self.input_manager.input_fields:
            if self.bound_inputs.get(f.name) is None and f.required:
                raise RuntimeError("Not all required inputs are bound")
            elif self.bound_inputs.get(f.name):
                required_blocks.append(self.bound_inputs[f.name].block_uuid)
        return required_blocks

    def get_user_actions(self):
        """
            @rtype: list of ActionRecord
        """
        return self._trans.user_visible(self.state)

    def to_dict(self):
        result = self._block_serializer.to_dict(self)
        #pprint(result)
        #import ipdb; ipdb.set_trace()
        return result

    def register_provided_objects(self, scope, scope_var):
        self.out_manager.register(scope_var.var_name, scope_var.data_type)
        scope.register_variable(scope_var)

    def apply_action_from_js(self, action_name, *args, **kwargs):
        if self._trans.is_action_available(self.state, action_name):
            self.do_action(action_name, *args, **kwargs)
        elif hasattr(self, action_name) and hasattr(getattr(self, action_name), "__call__"):
            getattr(self, action_name)(*args, **kwargs)
        else:
            raise RuntimeError("Block %s doesn't have action: %s" % (self.name, action_name))

    def do_action(self, action_name, exp, *args, **kwargs):
        next_state = self._trans.next_state(self.state, action_name)
        if next_state is not None:
            self.state = next_state
            exp.store_block(self)
            getattr(self, action_name)(exp, *args, **kwargs)

            # TODO: Check if self.scope_name is actually set to auto execution
            #
            if self.is_block_supports_auto_execution:
                if self.get_exec_status() == "done":
                    auto_exec_task.s(exp, self.scope_name).apply_async()

        else:
            raise RuntimeError("Action %s isn't available" % action_name)

    def change_base_name(self, exp, received_block, *args, **kwargs):
        # TODO: check if the name is correct
        new_name = received_block.get("base_name")

        if new_name:
            exp.change_block_alias(self, new_name)

    def save_params(self, exp, request, received_block=None, *args, **kwargs):
        self._block_serializer.save_params(self, received_block)
        exp.store_block(self)
        self.validate_params(exp)

    def validate_params(self, exp):
        is_valid = True
        #if self.form.is_valid():

        # check required inputs
        if not self.input_manager.validate_inputs(
                self.bound_inputs, self.errors, self.warnings):
            is_valid = False

        if True:
            self.errors = []
            self.do_action("on_params_is_valid", exp)
        else:
            self.do_action("on_params_not_valid", exp)

    def on_params_is_valid(self, exp):
        self.errors = []
        exp.store_block(self)

    def on_params_not_valid(self, exp):
        pass

    def clean_errors(self):
        self.errors = []

    def error(self, exp, new_errors=None):
        if isinstance(new_errors, collections.Iterable):
            self.errors.extend(new_errors)
        else:
            self.errors.append(new_errors)

        exp.store_block(self)

    def reset_execution(self, exp):
        pass

save_params_actions_list = ActionsList([
    ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                 user_title="Save parameters"),
    ActionRecord("on_params_is_valid", ["validating_params"], "ready"),
    ActionRecord("on_params_not_valid", ["validating_params"], "created"),
])

execute_block_actions_list = ActionsList([
    ActionRecord("execute", ["ready"], "working", user_title="Run block"),
    ActionRecord("success", ["working"], "done"),
    ActionRecord("error", ["ready", "working"], "execution_error"),
    ActionRecord("reset_execution", ['done'], "ready")
])


class GroupType(object):
    INPUT_DATA = "Input data"
    META_PLUGIN = "Meta plugins"
    VISUALIZE = "Visualize"
    CLASSIFIER = "Classifier"
    PROCESSING = "Data processing"
