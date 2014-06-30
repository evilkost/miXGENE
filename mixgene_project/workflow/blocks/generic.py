import abc
import logging
import collections
import itertools
from collections import defaultdict
from uuid import uuid1
import json

import redis_lock
from mixgene.redis_helper import ExpKeys
from mixgene.util import log_timing, get_redis_instance
from webapp.models import Experiment, UploadedData, UploadedFileWrapper
from webapp.notification import BlockUpdated
from webapp.scope import Scope, ScopeVar
from webapp.tasks import auto_exec_task, halt_execution_task
from workflow.blocks.fields import FieldType, BlockField, InputBlockField, \
    ActionRecord, ActionsList, MultiUploadField
from workflow.blocks.managers import TransSystem, BlockSerializer, OutManager, InputManager
from workflow.blocks.blocks_pallet import register_block, GroupType


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

taboo_attrs = [
    "__metaclass__",
    "is_abstract",
    "_trans",
    "_block_serializer"
]


class BlockMeta(abc.ABCMeta):
    def __new__(cls, name, bases, attrs):
        super_new = super(BlockMeta, cls).__new__
        module = attrs.pop('__module__')
        new_class = super_new(cls, name, bases, {'__module__': module})

        if hasattr(bases[0], "_block_serializer"):
            setattr(new_class, "_block_serializer", BlockSerializer.clone(bases[0]._block_serializer))
            setattr(new_class, "_trans", TransSystem.clone(bases[0]._trans))

        _attrs = {}
        for base in bases:
            for attr_name, attr_val in getattr(base, "_attrs", {}).iteritems():
                if attr_name not in taboo_attrs:
                    _attrs[attr_name] = attr_val

        _attrs.update(attrs)
        setattr(new_class, "_attrs", _attrs)

        for attr_name, attr_val in itertools.chain(attrs.iteritems(), _attrs.iteritems()):
            new_class.add_to_class(attr_name, attr_val)

        if not attrs.get("is_abstract", False):
            register_block(name, _attrs.get("name"), _attrs.get("block_group"), new_class)

        return new_class

    def add_to_class(cls, name, value):
        if hasattr(value, "contribute_to_class"):
            value.contribute_to_class(cls, name)
        else:
            setattr(cls, name, value)


class BaseBlock(object):
    _block_serializer = BlockSerializer()
    _trans = TransSystem()
    __attrs_collect = dict()
    is_abstract = True

    """
        WARNING: For blocks always inherit other *Block class at first position
    """
    __metaclass__ = BlockMeta


class GenericBlock(BaseBlock):
    # block fields
    is_abstract = True

    _uuid = BlockField("uuid", FieldType.STR, None, is_immutable=True)
    _name = BlockField("name", FieldType.STR, None)
    name = "Generic block"

    _base_name = BlockField("base_name", FieldType.STR, "", is_immutable=True)

    _block_group = BlockField("block_group", FieldType.STR, "", is_immutable=True)
    block_group = None

    _exp_id = BlockField("exp_id", FieldType.STR, None, is_immutable=True)

    _scope_name = BlockField("scope_name", FieldType.STR, "root", is_immutable=True)
    _sub_scope_name = BlockField("sub_scope_name", FieldType.STR, None, is_immutable=True)
    _visible_scopes_list = BlockField("visible_scopes_list",
                                      FieldType.SIMPLE_LIST, is_immutable=True)

    _state = BlockField("state", FieldType.STR, "created")

    _ui_folded = BlockField("ui_folded", FieldType.BOOLEAN, init_val=False)
    _ui_internal_folded = BlockField("ui_internal_folded", FieldType.BOOLEAN, init_val=False)
    _show_collector_editor = BlockField("show_collector_editor", FieldType.BOOLEAN, init_val=False)

    _has_custom_layout = BlockField("has_custom_layout", FieldType.BOOLEAN)
    _custom_layout_name = BlockField("custom_layout_name", FieldType.STR)

    _create_new_scope = BlockField("create_new_scope", FieldType.BOOLEAN)
    create_new_scope = False

    is_block_supports_auto_execution = False

    _errors = BlockField("errors", FieldType.SIMPLE_LIST, list())
    _warnings = BlockField("warnings", FieldType.SIMPLE_LIST, list())
    _bound_inputs = BlockField("bound_inputs", FieldType.SIMPLE_DICT, defaultdict())

    def __init__(self, exp_id=None, scope_name=None):
        """
            Building block for workflow
        """
        # TODO: due to dynamic inputs, find better solution
        self._block_serializer = BlockSerializer.clone(self.__class__._block_serializer)

        self.state = "created"
        self.uuid = "B" + uuid1().hex[:8]

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
            # TODO: Use factories for init values
            #if f.init_val is not None:
            #    setattr(self, f.name, f.init_val)

        scope.store()

        for f_name, f in self._block_serializer.fields.items():
            if f.init_val is not None:
                #setattr(self, f.name, f.init_val)
                pass

        for f_name, f in self._block_serializer.inputs.iteritems():
            self.input_manager.register(f)

    def on_remove(self, *args, **kwargs):
        """
            Cleanup all created files
            TODO: github:#61
        """
        pass


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
            @rtype: list of workflow.blocks.fields.ActionRecord
        """
        return self._trans.user_visible(self.state)

    @log_timing
    def to_dict(self):
        result = self._block_serializer.to_dict(self)
        # import ipdb; ipdb.set_trace()
        return result

    def register_provided_objects(self, scope, scope_var):
        self.out_manager.register(scope_var.var_name, scope_var.data_type)
        scope.register_variable(scope_var)

    @log_timing
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
            elif self.state in self.auto_exec_status_error \
                    and self.is_block_supports_auto_execution:
                log.debug("Detected error during automated workflow execution")
                halt_execution_task.s(exp, self.scope_name).apply_async()

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

    def save_file_input(self, exp, field_name, file_obj, multiple=False, upload_meta=None):
        if upload_meta is None:
            upload_meta = {}

        if not hasattr(self, field_name):
            raise Exception("Block doesn't have field: %s" % field_name)

        orig_name = file_obj.name
        local_filename = "%s_%s_%s" % (self.uuid[:8], field_name, file_obj.name)

        if not multiple:
            log.debug("Storing single upload to field: %s", field_name)
            ud, is_created = UploadedData.objects.get_or_create(
                exp=exp, block_uuid=self.uuid, var_name=field_name)

            file_obj.name = local_filename
            ud.data = file_obj
            ud.save()

            ufw = UploadedFileWrapper(ud.pk)
            ufw.orig_name = orig_name
            setattr(self, field_name, ufw)
            exp.store_block(self)
        else:
            log.debug("Adding upload to field: %s", field_name)

            ud, is_created = UploadedData.objects.get_or_create(
                exp=exp, block_uuid=self.uuid, var_name=field_name, filename=orig_name)

            file_obj.name = local_filename
            ud.data = file_obj
            ud.filename = orig_name
            ud.save()

            ufw = UploadedFileWrapper(ud.pk)
            ufw.orig_name = orig_name

            r = get_redis_instance()
            with redis_lock.Lock(r, ExpKeys.get_block_global_lock_key(self.exp_id, self.uuid)):
                log.debug("Enter lock, file: %s", orig_name)
                block = exp.get_block(self.uuid)
                attr = getattr(block, field_name)

                attr[orig_name] = ufw
                log.debug("Added upload `%s` to collection: %s", orig_name, attr.keys())
                exp.store_block(block)
                log.debug("Exit lock, file: %s", orig_name)

    def erase_file_input(self, exp, data):
        field_name = json.loads(data)["field_name"]
        field = self._block_serializer.params.get(field_name)

        if not field.options.get("multiple", False):
            #  single stored value
            ufw = getattr(self, field_name)
            ud = ufw.ud
            ud.delete()
            setattr(self, field_name, None)
        else:
            # multiple
            ufw_dict = getattr(self, field_name)
            for name, ufw in ufw_dict.items():
                ufw.ud.delete()
            setattr(self, field_name, MultiUploadField())

        exp.store_block(self)

    def add_dyn_input_hook(self, exp, dyn_port, new_port):
        """ to override later
        """
        pass

    def add_input_port(self, new_port):
        self._block_serializer.register(new_port)
        self.input_manager.register(new_port)

    def add_dyn_input(self, exp, received_block, *args, **kwargs):
        spec = received_block.get("_add_dyn_port")
        if not spec:
            return

        if not spec['new_port'] or not spec['input']:
            return

        dyn_port_name = spec['input']
        dyn_port = self._block_serializer.inputs.get(dyn_port_name)
        if not dyn_port:
            return

        order_num = 1000 + abs(dyn_port.order_num) * 10
        dp = getattr(self, dyn_port_name)
        if dp:
            order_num += len(dp)

        new_port = InputBlockField(
            name=spec['new_port'],
            required_data_type=dyn_port.required_data_type,
            order_num=order_num
        )

        self.add_input_port(new_port)
        getattr(self, dyn_port_name).append(spec["new_port"])

        self.add_dyn_input_hook(exp, dyn_port, new_port)
        exp.store_block(self)

    def validate_params_hook(self, exp, *args, **kwargs):
        return True

    def validate_params(self, exp, *args, **kwargs):
        is_valid = True

        # check required inputs
        if not self.input_manager.validate_inputs(
                self, self.bound_inputs, self.errors, self.warnings):
            is_valid = False
        # check user provided values
        if not self._block_serializer.validate_params(self, exp):
            is_valid = False

        if not self.validate_params_hook(exp, *args, **kwargs):
            is_valid = False

        if is_valid:
            self.errors = []
            self.do_action("on_params_is_valid", exp)
        else:
            self.do_action("on_params_not_valid", exp)

    def on_params_is_valid(self, exp, *args, **kwargs):
        self.errors = []
        exp.store_block(self)

    def on_params_not_valid(self, exp, *args, **kwargs):
        pass

    def clean_errors(self):
        self.errors = []

    def error(self, exp, new_errors=None):
        if isinstance(new_errors, collections.Iterable):
            self.errors.extend(new_errors)
        elif new_errors:
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


