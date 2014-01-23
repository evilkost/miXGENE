from workflow.execution import ExecStatus

__author__ = 'kost'

from uuid import uuid1
from workflow.ports import BlockPort, BoundVar

class GroupType(object):
    INPUT_DATA = "Input data"
    META_PLUGIN = "Meta plugins"
    VISUALIZE = "Visualize"
    CLASSIFIER = "Classifier"
    PROCESSING = "Data processing"


class GenericBlock(object):
    block_base_name = "GENERIC_BLOCK"
    provided_objects = {}
    provided_objects_inner = {}
    create_new_scope = False
    sub_scope = None
    is_base_name_visible = True
    params_prototype = {}

    pages = {}
    is_sub_pages_visible = False

    elements = []

    exec_status_map = {}

    def __init__(self, name, exp_id, scope):
        """
            Building block for workflow
            @type can be: "user_input", "computation"
        """
        self.uuid = uuid1().hex
        self.name = name
        self.exp_id = exp_id

        # pairs of (var name, data type, default name in context)
        self.required_inputs = []
        self.provide_outputs = []

        self.state = "created"

        self.errors = []
        self.warnings = []
        self.base_name = ""

        self.scope = scope
        self.ports = {}  # {group_name -> [BlockPort1, BlockPort2]}
        self.params = {}

    @property
    def sub_blocks(self):
        return []

    def clean_errors(self):
        self.errors = []

    def get_available_user_action(self):
        return self.get_allowed_actions(True)

    def get_exec_status(self):
        return self.exec_status_map.get(self.state, ExecStatus.USER_REQUIRED)

    def get_allowed_actions(self, only_user_actions=False):
        # TODO: REFACTOR!!!!!
        action_list = []
        for line in self.all_actions:
            # action_code, action_title, user_visible = line

            action_code = line[0]
            user_visible = line[2]
            self.fsm.current = self.state

            if self.fsm.can(action_code) and \
                    (not only_user_actions or user_visible):
                action_list.append(line)
        return action_list

    def do_action(self, action_name, *args, **kwargs):
        #TODO: add notification to html client
        if action_name in [row[0] for row in self.get_allowed_actions()]:
            self.fsm.current = self.state
            getattr(self.fsm, action_name)()
            self.state = self.fsm.current
            print "change state to: %s" % self.state
            getattr(self, action_name)(*args, **kwargs)
        else:
            raise RuntimeError("Action %s isn't available" % action_name)

    def before_render(self, exp, *args, **kwargs):
        """
        Invoke prior to template applying, prepare relevant data
        @param exp: Experiment
        @return: additional content for template context
        """
        self.collect_port_options(exp)
        return {}

    def bind_variables(self, exp, request, received_block):
        # TODO: Rename to bound inner variables, or somehow detect only changed variables
        #pprint(received_block)
        for port_group in ['input', 'collect_internal']:
            if port_group in self.ports:
                for port_name in self.ports[port_group].keys():
                    port = self.ports[port_group][port_name]
                    received_port = received_block['ports'][port_group][port_name]
                    port.bound_key = received_port.get('bound_key')

        exp.store_block(self)

    def save_form(self, exp, request, received_block=None, *args, **kwargs):
        if received_block is None:
            self.form = self.form_cls(request.POST)
            self.validate_form()
        else:
            #import ipdb; ipdb.set_trace()
            self.params = received_block['params']
            self.form = self.form_cls(received_block['params'])
            self.validate_form()
        exp.store_block(self)

    def validate_form(self):
        if self.form.is_valid():
            #TODO: additional checks e.g. other blocks doesn't provide
            #      variables with the same names
            self.errors = []
            self.do_action("on_form_is_valid")
        else:
            self.do_action("on_form_not_valid")

    def on_form_is_valid(self):
        self.errors = []

    def on_form_not_valid(self):
        pass

    def serialize(self, exp, to="dict"):
        self.before_render(exp)
        if to == "dict":
            keys_to_snatch = {"uuid", "base_name", "name",
                              "scope", "sub_scope", "create_new_scope",
                              "warnings", "state",
                              "params_prototype",  # TODO: make ParamProto class and genrate BlockForm
                              #  and params_prototype with metaclass magic
                              "params",
                              "pages", "is_sub_pages_visible", "elements",
                              }
            hash = {}
            for key in keys_to_snatch:
                hash[key] = getattr(self, key)

            hash['ports'] = {
                group_name: {
                    port_name: port.serialize()
                    for port_name, port in group_ports.iteritems()
                }
                for group_name, group_ports in self.ports.iteritems()
            }
            hash['actions'] = [
                {
                    "code": action_code,
                    "title": action_title
                }
                for action_code, action_title, _ in
                self.get_available_user_action()
            ]

            if hasattr(self, 'form') and self.form is not None:
                hash['form_errors'] = self.form.errors

            hash['errors'] = []
            for err in self.errors:
                hash['errors'].append(str(err))

            return hash

    @staticmethod
    def get_var_by_bound_key_str(exp, bound_key_str):
        uuid, field = bound_key_str.split(":")
        block = exp.get_block(uuid)
        return getattr(block, field)

    def collect_port_options(self, exp):
        """
        @type exp: Experiment
        """
        variables = exp.get_registered_variables()

        aliases_map = exp.get_block_aliases_map()
        # structure: (scope, uuid, var_name, var_data_type)
        for group_name, port_group in self.ports.iteritems():
            for port_name, port in port_group.iteritems():
                port.options = {}
                if port.bound_key is None:
                    for scope, uuid, var_name, var_data_type in variables:
                        if uuid == self.uuid:
                            continue
                        if scope in port.scopes and var_data_type == port.data_type:
                            port.bound_key = BoundVar(
                                block_uuid=uuid,
                                block_alias=aliases_map[uuid],
                                var_name=var_name
                            ).key
                            break

                            # for scope, uuid, var_name, var_data_type in variables:
                            #     if scope in port.scopes and var_data_type == port.data_type:
                            #         var = BoundVar(
                            #             block_uuid=uuid,
                            #             block_alias=aliases_map[uuid],
                            #             var_name=var_name
                            #         )
                            #         port.options[var.key] = var
                            # if port.bound_key is not None and port.bound_key not in port.options.keys():
                            #     port.bound_key = None
