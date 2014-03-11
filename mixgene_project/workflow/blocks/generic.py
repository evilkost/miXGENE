import logging
from pprint import pprint
import collections
import copy
import itertools
import random

from webapp.models import Experiment, UploadedData, UploadedFileWrapper
from webapp.notification import Notification, NotifyType, BlockUpdated
from webapp.scope import Scope, ScopeVar

from collections import defaultdict
from uuid import uuid1
from webapp.tasks import auto_exec_task
from workflow.blocks.errors import InputPortError

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

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
    def __init__(self, required_data_type=None, multiply_extensible=False, *args, **kwargs):
        super(InputBlockField, self).__init__(*args, **kwargs)
        self.required_data_type = required_data_type
        self.field_type = FieldType.INPUT_PORT
        self.bound_var_key = None
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


# TODO: maybe more use of django form fields?
# TODO: or join ParamField and BlockField?
class ParamField(object):
    def __init__(self, name, title, input_type, field_type, init_val=None,
                 validator=None, select_provider=None,
                 required=True, order_num=None,
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
        if order_num is None:
            self.order_num = random.randint(0, 1000)
        else:
            self.order_num = order_num

    def contribute_to_class(self, cls, name):
        #setattr(cls, name, self.init_val)
        getattr(cls, "_block_serializer").register(self)

    def to_dict(self):
        return {k: unicode(v) for k, v in self.__dict__.iteritems()}

    def value_to_dict(self, raw_val, block):
        val = str(raw_val)
        try:
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
        except Exception, e:
            pprint(e)
            # TODO: fix it
            return ""

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
        bs.outputs = copy.deepcopy(other.outputs)
        bs.inner_outputs = copy.deepcopy(other.inner_outputs)
        bs.inputs = copy.deepcopy(other.inputs)
        return bs

    def to_dict(self, block):
        result = {}
        for f_name, f in self.fields.iteritems():
            if f.field_type == FieldType.HIDDEN:
                continue
            raw_val = getattr(block, f_name)
            result[f_name] = f.value_to_dict(raw_val, block)

        params_protype = {
            str(param_name): param_field.to_dict()
            for param_name, param_field in self.params.iteritems()
        }
        result["_params_prototype"] = params_protype
        result["_params_prototype_list"] = params_protype.values()

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
        # import  ipdb; ipdb.set_trace()
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


class CollectorSpecification(object):
    def __init__(self):
        self.bound = {}  # name -> scope_var

    def register(self, name, scope_var):
        """
            @type scope_var: ScopeVar
        """
        # if not isinstance(scope_var, ScopeVar):
        #     pprint(scope_var)
        #     import  ipdb; ipdb.set_trace()

        self.bound[name] = scope_var

    def to_dict(self, *args, **kwargs):
        return {
            "bound": {str(name): scope_var.to_dict()
                      for name, scope_var in self.bound.iteritems()},
            "new": {"name": "", "scope_var": ""}
        }


class BlockMeta(type):
    def __new__(cls, name, bases, attrs):
        # print "BlockMeta new: %s " % name
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

    def validate_inputs(self, block, bound_inputs, errors, warnings):
        is_valid = True
        for f in self.input_fields:
            if f.multiply_extensible:
                continue
            if bound_inputs.get(f.name) is None:
                exception = InputPortError(block, f.name, "Input port not bound")
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
    _visible_scopes_list = BlockField("visible_scopes_list",
                                      FieldType.SIMPLE_LIST, is_immutable=True)

    state = BlockField("state", FieldType.STR, "created")

    ui_folded = BlockField("ui_folded", FieldType.BOOLEAN, init_val=False)

    create_new_scope = False
    _create_new_scope = BlockField("create_new_scope", FieldType.BOOLEAN, False)
    _collector_spec = ParamField(name="collector_spec", title="",
                                 field_type=FieldType.CUSTOM,
                                 input_type=InputType.HIDDEN,
                                 init_val=CollectorSpecification()
    )


    is_block_supports_auto_execution = False

    errors = BlockField("errors", FieldType.SIMPLE_LIST, list())
    warnings = BlockField("warnings", FieldType.SIMPLE_LIST, list())
    bound_inputs = BlockField("bound_inputs", FieldType.SIMPLE_DICT, defaultdict())

    def __init__(self, name, exp_id, scope_name):
        """
            Building block for workflow
        """
        # TODO: due to dynamic inputs, find better solution
        self._block_serializer = BlockSerializer.clone(self.__class__._block_serializer)

        self.state = "created"
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

        # Init block fields
        for f_name, f in itertools.chain(
                self._block_serializer.fields.iteritems(),
                self._block_serializer.params.iteritems()):

            #if f_name not in self.__dict__ and not f.is_a_property:
            if not f.is_a_property and not hasattr(self, f_name):
                try:
                    setattr(self, f_name, f.init_val)
                except:
                    import ipdb; ipdb.set_trace()

        for f_name, f in self._block_serializer.inputs.iteritems():
            if f.multiply_extensible:
                setattr(self, f_name, [])  # Names of dynamically added ports

        # TODO: Hmm maybe more metaclass magic can be applied here
        scope = self.get_scope()
        scope.load()
        for f_name, f in self._block_serializer.outputs.iteritems():
            log.debug("Registering normal outputs: %s", f_name)
            self.register_provided_objects(scope, ScopeVar(self.uuid, f_name, f.provided_data_type))
            # TODO: User factories for init values
            # if f.init_val is not None:
            #     setattr(self, f.name, f.init_val)
        scope.store()

        if hasattr(self, "create_new_scope") and self.create_new_scope:
            log.debug("Trying to add inner outputs for block %s in exp: %s",
                      self.name, self.exp_id)
            scope = self.get_sub_scope()
            scope.load()
            for f_name, f in self._block_serializer.inner_outputs.iteritems():
                log.debug("Registering inner outputs: %s in block: %s, exp: %s",
                          f_name, self.name, self.exp_id)
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
        log.debug("bound input %s to %s in block: %s, exp: %s",
                  input_name, bound_var, self.base_name, self.exp_id)
        self.bound_inputs[input_name] = bound_var

    def get_input_var(self, name):
        try:
            exp = Experiment.get_exp_by_id(self.exp_id)
            scope_var = self.bound_inputs[name]
            return exp.get_scope_var_value(scope_var)
        except:
            return None

    def get_out_var(self, name):
        if self.out_manager.contains(name):
            return self._out_data.get(name)
        elif self.create_new_scope:
            return self.get_inner_out_var(name)
        else:
            return None

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

    @property
    def visible_scopes_list(self):
        scope = self.get_scope()
        scope_names_list = scope.get_parent_scope_list()
        scope_names_list.append(self.scope_name)
        return scope_names_list

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
            if f.multiply_extensible:
                continue
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
        # import ipdb; ipdb.set_trace()
        return result

    def register_collector_bind(self, name, scope_var):
        self.collector_spec.register(name, scope_var)

    def register_provided_objects(self, scope, scope_var):
        self.out_manager.register(scope_var.var_name, scope_var.data_type)
        scope.register_variable(scope_var)

    def apply_action_from_js(self, action_name, *args, **kwargs):
        if self._trans.is_action_available(self.state, action_name):
            self.do_action(action_name, *args, **kwargs)
        elif hasattr(self, action_name) and hasattr(getattr(self, action_name), "__call__"):
            return getattr(self, action_name)(*args, **kwargs)
        else:
            raise RuntimeError("Block %s doesn't have action: %s" % (self.name, action_name))

    def do_action(self, action_name, exp, *args, **kwargs):
        # if action_name == "success" and self.block_base_name == "CROSS_VALID":
        #     from celery.contrib import rdb; rdb.set_trace()
        ar = self._trans.action_records_by_name[action_name]
        old_exec_state = self.get_exec_status()
        next_state = self._trans.next_state(self.state, action_name)

        if next_state is not None:
            log.debug("Do action: %s in block %s from state %s -> %s",
                      action_name, self.base_name, self.state, next_state)
            self.state = next_state

            if old_exec_state != "done" and self.get_exec_status() == "done":
                if self.is_block_supports_auto_execution:
                    BlockUpdated(self.exp_id,
                                 block_uuid=self.uuid, block_alias=self.base_name,
                                 silent=True).send()
            exp.store_block(self)
            getattr(self, action_name)(exp, *args, **kwargs)

            if ar.reload_block_in_client:
                BlockUpdated(self.exp_id, self.uuid, self.base_name).send()

            # TODO: Check if self.scope_name is actually set to auto execution
            #
            if old_exec_state != "done" and self.get_exec_status() == "done" \
                    and ar.propagate_auto_execution \
                    and self.is_block_supports_auto_execution:
                log.debug("Propagate execution: %s ", self.base_name)
                auto_exec_task.s(exp, self.scope_name).apply_async()

        else:
            raise RuntimeError("Action %s isn't available for block %s in state %s" %
                               (action_name, self.base_name, self.state))

    def change_base_name(self, exp, received_block, *args, **kwargs):
        # TODO: check if the name is correct
        new_name = received_block.get("base_name")

        if new_name:
            exp.change_block_alias(self, new_name)

    def toggle_ui_folded(self, exp, received_block, *args, **kwargs):
        self.ui_folded = received_block["ui_folded"]
        exp.store_block(self)

    def save_params(self, exp, received_block=None, *args, **kwargs):
        self._block_serializer.save_params(self, received_block)
        exp.store_block(self)
        self.validate_params(exp)

    def save_file_input(self, exp, field_name, file_obj, upload_meta=None):
        if upload_meta is None:
            upload_meta = {}

        if not hasattr(self, field_name):
            raise Exception("Block doesn't have field: %s" % field_name)

        ud, is_created = UploadedData.objects.get_or_create(
            exp=exp, block_uuid=self.uuid, var_name=field_name)
        # import ipdb; ipdb.set_trace()
        orig_name = file_obj.name

        local_filename = "%s_%s_%s" % (self.uuid[:8], field_name, file_obj.name)
        file_obj.name = local_filename
        ud.data = file_obj
        ud.save()

        ufw = UploadedFileWrapper(ud.pk)
        ufw.orig_name = orig_name
        setattr(self, field_name, ufw)

        exp.store_block(self)

    def add_collector_var(self, exp, received_block, *args, **kwargs):
        rec_new = received_block.get("collector_spec", {}).get("new", {})
        if rec_new:
            name = str(rec_new.get("name"))
            scope_var_key = rec_new.get("scope_var")
            data_type = rec_new.get("data_type")
            if name and scope_var_key:
                scope_var = ScopeVar.from_key(scope_var_key)
                scope_var.data_type = data_type
                self.register_collector_bind(name, scope_var)
                exp.store_block(self)


    def add_dyn_input_hook(self, exp, dyn_port, new_port):
        """ to override later
        """
        pass

    def add_dyn_input(self, exp, received_block, *args, **kwargs):
        # {u'_add_dyn_port': {u'input': },
        #  u'new_port': {u'name': u'sad'}}

        spec = received_block.get("_add_dyn_port")
        if not spec:
            return

        if not spec['new_port'] or not spec['input']:
            return

        dyn_port_name = spec['input']
        dyn_port = self._block_serializer.inputs.get(dyn_port_name)
        if not dyn_port:
            return

        new_port = InputBlockField(name=spec['new_port'],
                                   required_data_type=dyn_port.required_data_type)
        self._block_serializer.register(new_port)
        self.input_manager.register(new_port)

        getattr(self, dyn_port_name).append(spec["new_port"])

        #import ipdb; ipdb.set_trace()
        # adding to
        #port = InputBlockField
        #
        self.add_dyn_input_hook(exp, dyn_port, new_port)
        exp.store_block(self)


    def validate_params(self, exp):
        is_valid = True

        # check required inputs
        if not self.input_manager.validate_inputs(
                self.bound_inputs, self.errors, self.warnings):
            is_valid = False
        # check user provided values
        if not self._block_serializer.validate_params(self, exp):
            is_valid = False

        if is_valid:
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

    def reset_execution(self, exp, *args, **kwargs):
        self.clean_errors()
        exp.store_block(self)


save_params_actions_list = ActionsList([
    ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                 user_title="Save parameters"),
    ActionRecord("on_params_is_valid", ["validating_params"], "ready"),
    ActionRecord("on_params_not_valid", ["validating_params"], "created"),
])

execute_block_actions_list = ActionsList([
    ActionRecord("execute", ["ready"], "working", user_title="Run block"),
    ActionRecord("success", ["working"], "done", propagate_auto_execution=True),
    ActionRecord("error", ["*", "ready", "working"], "execution_error"),
    ActionRecord("reset_execution", ["*", "done", "execution_error", "ready", "working"], "ready",
                 user_title="Reset execution")
])


class GroupType(object):
    INPUT_DATA = "Input data"
    META_PLUGIN = "Meta plugins"
    VISUALIZE = "Visualize"
    CLASSIFIER = "Classifier"
    PROCESSING = "Data processing"


class IteratedInnerFieldManager(object):
    def __init__(self):
        self.fields = {}
        self.sequence = []
        self.iterator = -1

    def register(self, field):
        """
            @type field: BlockField
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