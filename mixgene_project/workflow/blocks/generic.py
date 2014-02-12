import copy
from webapp.models import Experiment
from webapp.scope import Scope, ScopeVar
from workflow.execution import ExecStatus

from uuid import uuid1
from workflow.ports import BoundVar



from collections import defaultdict
from uuid import uuid1


class ActionRecord(object):
    def __init__(self, name, src_states, dst_state, user_title=None, **kwargs):
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
                 is_immutable=False, required=False,
                 *args, **kwargs):
        self.name = name
        self.field_type = field_type
        self.init_val = init_val
        self.is_immutable = is_immutable
        self.required = required

    def contribute_to_class(self, cls, name):
        setattr(cls, name, self.init_val)
        getattr(cls, "_block_serializer").register(self)


class OutputBlockField(BlockField):
    def __init__(self, provided_data_type=None, *args, **kwargs):
        super(OutputBlockField, self).__init__(*args, **kwargs)
        self.provided_data_type = provided_data_type

    def contribute_to_class(self, cls, name):
        getattr(cls, "_block_serializer").register(self)


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
    CHOICE = "choice"
    CHECKBOX = "checkbox"


# TODO: maybe more use of django form fields?
# TODO: or join ParamField and BlockField?
class ParamField(object):
    def __init__(self, name, title, input_type, field_type, init_val,
                 validator=None, *args, **kwargs):
        self.name = name
        self.title = title
        self.input_type = input_type
        self.field_type = field_type
        self.init_val = init_val
        self.validator = validator

    def contribute_to_class(self, cls, name):
        setattr(cls, name, self.init_val)
        getattr(cls, "_block_serializer").register(self)

    def to_dict(self):
        return {
            "name": self.name,
            "title": self.title,
            "field_type": self.field_type,
            "input_type": self.input_type,
            "init_val": self.init_val,
            "validator": self.validator,
        }


class BlockSerializer(object):
    def __init__(self):
        self.fields = dict()
        self.params = dict()
        self.outputs = dict()
        self.inputs = dict()

    def register(self, field):
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
            if raw_val is None:
                result[f_name] = None
            else:
                if f.field_type == FieldType.RAW:
                    result[f_name] = raw_val
                if f.field_type == FieldType.CUSTOM:
                    result[f_name] = raw_val.to_dict(block)
                if f.field_type in [FieldType.STR, FieldType.INT, FieldType.FLOAT, FieldType.BOOLEAN]:
                    result[f_name] = str(raw_val)
                if f.field_type == FieldType.SIMPLE_DICT:
                    result[f_name] = {str(k): str(v) for k, v in raw_val.iteritems()}
                if f.field_type == FieldType.SIMPLE_LIST:
                    result[f_name] = map(str, raw_val)

        result["_params_prototype"] = dict([(str(param_name), param_field.to_dict())
                                   for param_name, param_field in self.params.iteritems()])

        for p_name, p in self.params.iteritems():
            raw_val = getattr(block, p_name)
            if p.input_type == InputType.TEXT:
                result[p_name] = str(raw_val)

        result["actions"] = [{
            "code": ar.name,
            "title": ar.user_title,
        } for ar in block.get_user_actions()]

        result["out"] = block.out_manager.to_dict()
        result["inputs"] = block.input_manager.to_dict()

        return result

    def save_params(self, block, received_block):
        """
            @param block: GenericBlock
            @param received_block: dict
        """
        for p_name, p in self.params.iteritems():
            # TODO: here invoke validator
            setattr(block, p_name, received_block.get(p_name))

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

    def register(self, name, data_type):
        if name in self.data_type_by_name.keys():
            raise KeyError("Field with name %s already exists" % name)

        self.data_type_by_name[name] = data_type
        self.fields_by_data_type[data_type].append(name)

    def get_fields_by_data_type(self, data_type):
        return self.fields_by_data_type[data_type]

    def to_dict(self):
        return {}


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

    scope = BlockField("scope", FieldType.STR, "root", is_immutable=True)
    state = BlockField("state", FieldType.STR, "created")

    create_new_scope = BlockField("create_new_scope", FieldType.BOOLEAN, False)

    errors = BlockField("errors", FieldType.SIMPLE_LIST, list())
    warnings = BlockField("warnings", FieldType.SIMPLE_LIST, list())
    bound_inputs = BlockField("bound_inputs", FieldType.SIMPLE_DICT, dict())

    def __init__(self, name, exp_id, scope_name):
        """
            Building block for workflow
        """
        self.uuid = uuid1().hex
        self.name = name
        self.exp_id = exp_id
        self.scope_name = scope_name

        self.base_name = ""

        self.ports = {}  # {group_name -> [BlockPort1, BlockPort2]}
        self.params = {}

        self._out_data = dict()

        self.out_manager = OutManager()

        # self.bound_inputs = {}
        self.input_manager = InputManager()

        # TODO: Hmm maybe more metaclass magic can be applied here
        scope = self.get_scope()
        scope.load()
        for f_name, f in self._block_serializer.outputs.iteritems():
            self.register_provided_objects(scope, ScopeVar(self.uuid, f_name, f.provided_data_type))
        scope.store()

        for f_name, f in self._block_serializer.inputs.iteritems():
            if f.name not in self.bound_inputs:
                self.bound_inputs[f.name] = None
            self.input_manager.register(f)

    def bind_input_var(self, input_name, bound_var):
        self.bound_inputs[input_name] = bound_var

    def get_input_var(self, name):
        exp = Experiment.get_exp_by_id(self.exp_id)
        scope_var = self.bound_inputs[name]
        return exp.get_scope_var_value(scope_var)

    def get_out_var(self, name):
        return self._out_data.get(name)

    def set_out_var(self, name, value):
        self._out_data[name] = value

    def get_scope(self):
        exp = Experiment.get_exp_by_id(self.exp_id)
        return Scope(exp, self.scope_name)

    def get_user_actions(self):
        """
            @rtype: list of ActionRecord
        """
        return self._trans.user_visible(self.state)

    def to_dict(self):
        # import ipdb; ipdb.set_trace()
        result = self._block_serializer.to_dict(self)
        print result
        return result


    def register_provided_objects(self, scope, scope_var):
        self.out_manager.register(scope_var.var_name, scope_var.data_type)
        scope.register_variable(scope_var)

    def do_action(self, action_name, *args, **kwargs):
        # import ipdb; ipdb.set_trace()
        next_state = self._trans.next_state(self.state, action_name)
        if next_state is not None:
            self.state = next_state
            getattr(self, action_name)(*args, **kwargs)
        else:
            raise RuntimeError("Action %s isn't available" % action_name)

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
        exp.store_block(self)

    def clean_errors(self):
        self.errors = []

save_params_actions_list = ActionsList([
    ActionRecord("save_params", ["created", "params_modified", "valid_params", "done"], "validating_params",
                 user_title="Save parameters"),
    ActionRecord("on_params_is_valid", ["validating_params"], "valid_params"),
    ActionRecord("on_params_not_valid", ["validating_params"], "params_modified"),
])

execute_block_actions_list = ActionsList([
    ActionRecord("execute", ["ready"], "working", user_title="Run block"),
    ActionRecord("success", ["working"], "done"),
    ActionRecord("failure", ["working"], "ready"),
])


class Y(GenericBlock):
    _save_form_actions = save_params_actions_list


class Z(GenericBlock):
    _save_form_actions = save_params_actions_list

if __name__ == "__main__":
    y = Y(1, 2, 3)
    print(y.to_dict())
    y.do_action("save_form")
    print(y.to_dict())


class GroupType(object):
    INPUT_DATA = "Input data"
    META_PLUGIN = "Meta plugins"
    VISUALIZE = "Visualize"
    CLASSIFIER = "Classifier"
    PROCESSING = "Data processing"

#
# class GenericBlockOld(object):
#     block_base_name = "GENERIC_BLOCK"
#     provided_objects = {}
#     provided_objects_inner = {}
#     create_new_scope = False
#     sub_scope = None
#     is_base_name_visible = True
#     params_prototype = {}
#
#     pages = {}
#     is_sub_pages_visible = False
#
#     elements = []
#
#     exec_status_map = {}
#
#     def __init__(self, name, exp_id, scope):
#         """
#             Building block for workflow
#         """
#         self.uuid = uuid1().hex
#         self.name = name
#         self.exp_id = exp_id
#
#         # pairs of (var name, data type, default name in context)
#         self.required_inputs = []
#         self.provide_outputs = []
#
#         self.state = "created"
#
#         self.errors = []
#         self.warnings = []
#         self.base_name = ""
#
#         self.scope = scope
#         self.ports = {}  # {group_name -> [BlockPort1, BlockPort2]}
#         self.params = {}
#
#     @property
#     def sub_blocks(self):
#         return []
#
#     def clean_errors(self):
#         self.errors = []
#
#     def get_available_user_action(self):
#         return self.get_allowed_actions(True)
#
#     def get_exec_status(self):
#         return self.exec_status_map.get(self.state, ExecStatus.USER_REQUIRED)
#
#     def get_allowed_actions(self, only_user_actions=False):
#         # TODO: REFACTOR!!!!!
#         action_list = []
#         for line in self.all_actions:
#             # action_code, action_title, user_visible = line
#
#             action_code = line[0]
#             user_visible = line[2]
#             self.fsm.current = self.state
#
#             if self.fsm.can(action_code) and \
#                     (not only_user_actions or user_visible):
#                 action_list.append(line)
#         return action_list
#
#     def do_action(self, action_name, *args, **kwargs):
#         #TODO: add notification to html client
#         if action_name in [row[0] for row in self.get_allowed_actions()]:
#             self.fsm.current = self.state
#             getattr(self.fsm, action_name)()
#             self.state = self.fsm.current
#             print "change state to: %s" % self.state
#             getattr(self, action_name)(*args, **kwargs)
#         else:
#             raise RuntimeError("Action %s isn't available" % action_name)
#
#     def before_render(self, exp, *args, **kwargs):
#         """
#         Invoke prior to template applying, prepare relevant data
#         @param exp: Experiment
#         @return: additional content for template context
#         """
#         self.collect_port_options(exp)
#         return {}
#
#     def bind_variables(self, exp, request, received_block):
#         # TODO: Rename to bound inner variables, or somehow detect only changed variables
#         #pprint(received_block)
#         for port_group in ['input', 'collect_internal']:
#             if port_group in self.ports:
#                 for port_name in self.ports[port_group].keys():
#                     port = self.ports[port_group][port_name]
#                     received_port = received_block['ports'][port_group][port_name]
#                     port.bound_key = received_port.get('bound_key')
#
#         exp.store_block(self)
#
#     def save_params(self, exp, request, received_block=None, *args, **kwargs):
#         self._block_serializer.save_params(received_block)
#         exp.store_block(self)
#         self.validate_params()
#
#     def validate_params(self):
#         if self.form.is_valid():
#             self.errors = []
#             self.do_action("on_form_is_valid")
#         else:
#             self.do_action("on_form_not_valid")
#
#     def on_form_is_valid(self):
#         self.errors = []
#
#     def on_form_not_valid(self):
#         pass
#
#     def serialize(self, exp, to="dict"):
#         self.before_render(exp)
#         if to == "dict":
#             keys_to_snatch = {"uuid", "base_name", "name",
#                               "scope", "sub_scope", "create_new_scope",
#                               "warnings", "state",
#                               "params_prototype",  # TODO: make ParamProto class and genrate BlockForm
#                               #  and params_prototype with metaclass magic
#                               "params",
#                               "pages", "is_sub_pages_visible", "elements",
#                               }
#             hash = {}
#             for key in keys_to_snatch:
#                 hash[key] = getattr(self, key)
#
#             hash['ports'] = {
#                 group_name: {
#                     port_name: port.serialize()
#                     for port_name, port in group_ports.iteritems()
#                 }
#                 for group_name, group_ports in self.ports.iteritems()
#             }
#             hash['actions'] = [
#                 {
#                     "code": action_code,
#                     "title": action_title
#                 }
#                 for action_code, action_title, _ in
#                 self.get_available_user_action()
#             ]
#
#             if hasattr(self, 'form') and self.form is not None:
#                 hash['form_errors'] = self.form.errors
#
#             hash['errors'] = []
#             for err in self.errors:
#                 hash['errors'].append(str(err))
#
#             return hash
#
#     @staticmethod
#     def get_var_by_bound_key_str(exp, bound_key_str):
#         uuid, field = bound_key_str.split(":")
#         block = exp.get_block(uuid)
#         return getattr(block, field)
#
#     def collect_port_options(self, exp):
#         """
#         @type exp: Experiment
#         """
#         variables = exp.get_registered_variables()
#
#         aliases_map = exp.get_block_aliases_map()
#         # structure: (scope, uuid, var_name, var_data_type)
#         for group_name, port_group in self.ports.iteritems():
#             for port_name, port in port_group.iteritems():
#                 port.options = {}
#                 if port.bound_key is None:
#                     for scope, uuid, var_name, var_data_type in variables:
#                         if uuid == self.uuid:
#                             continue
#                         if scope in port.scopes and var_data_type == port.data_type:
#                             port.bound_key = BoundVar(
#                                 block_uuid=uuid,
#                                 block_alias=aliases_map[uuid],
#                                 var_name=var_name
#                             ).key
#                             break
#
#                             # for scope, uuid, var_name, var_data_type in variables:
#                             #     if scope in port.scopes and var_data_type == port.data_type:
#                             #         var = BoundVar(
#                             #             block_uuid=uuid,
#                             #             block_alias=aliases_map[uuid],
#                             #             var_name=var_name
#                             #         )
#                             #         port.options[var.key] = var
#                             # if port.bound_key is not None and port.bound_key not in port.options.keys():
#                             #     port.bound_key = None
