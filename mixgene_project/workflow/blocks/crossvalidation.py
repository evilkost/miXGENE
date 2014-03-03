from copy import deepcopy
from pprint import pprint
from mixgene.redis_helper import ExpKeys

from webapp.models import Experiment
from environment.structures import SequenceContainer
from webapp.scope import ScopeRunner, ScopeVar
from workflow.common_tasks import generate_cv_folds, wrapper_task
from generic import InnerOutputField
from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, OutputBlockField, InputBlockField, IteratedInnerFieldManager
from mixgene.util import get_redis_instance
import redis_lock

# class CrossValidationForm(forms.Form):
#     folds_num = forms.IntegerField(min_value=2, max_value=100)
#     #split_ratio = forms.FloatField(min_value=0, max_value=1)
#


class CrossValidation(GenericBlock):
    block_base_name = "CROSS_VALID"
    create_new_scope = True
    is_block_supports_auto_execution = True

    _block_actions = ActionsList([])
    _block_actions.extend(save_params_actions_list)

    _block_actions.extend(ActionsList([
        ActionRecord("execute", ["ready"], "generating_folds", user_title="Run block"),

        ActionRecord("on_folds_generation_success", ["generating_folds"], "ready_to_run_sub_scope", reload_block_in_client=True),

        ActionRecord("run_sub_scope", ["ready_to_run_sub_scope"], "sub_scope_executing"),
        ActionRecord("on_sub_scope_done", ["sub_scope_executing"], "ready_to_run_sub_scope"),

        ActionRecord("continue_collecting_sub_scope", ["ready_to_run_sub_scope"],
                     "sub_scope_executing"),

        ActionRecord("success", ["working", "ready_to_run_sub_scope"], "done",
                     propagate_auto_execution=True, reload_block_in_client=True),
        ActionRecord("error", ["ready", "working", "sub_scope_executing", "generating_folds"],
                     "execution_error", reload_block_in_client=True),

        ActionRecord("reset_execution", ["*", 'done', "sub_scope_executing", "ready",
                                         "generating_folds", "execution_error"], "ready",
                     user_title="Reset execution"),
    ]))

    elements = BlockField(name="elements", field_type=FieldType.SIMPLE_LIST, init_val=[
        "cv_info.html"
    ])

    folds_num = ParamField(name="folds_num", title="Folds number",
                           input_type=InputType.TEXT, field_type=FieldType.INT, init_val=10)

    _input_es_dyn = InputBlockField(
        name="es_inputs", required_data_type="ExpressionSet",
        required=True, multiply_extensible=True
    )

    _res_seq = OutputBlockField(name="res_seq", provided_data_type="SequenceContainer",
                                field_type=FieldType.CUSTOM, init_val=SequenceContainer())

    def __init__(self, *args, **kwargs):
        super(CrossValidation, self).__init__("Cross Validation", *args, **kwargs)
        self.auto_exec_status_working.update(["sub_scope_executing", "ready_to_run_sub_scope",
                                              "generating_folds"])

        self.celery_task = None
        self.inner_output_es_names_map = {}

        self.inner_output_manager = IteratedInnerFieldManager()
        for f_name, f in self._block_serializer.inner_outputs.iteritems():
            self.inner_output_manager.register(f)

        self.set_out_var("res_seq", SequenceContainer())

    def add_dyn_input_hook(self, exp, dyn_port, new_port):
        """
            @type new_port: InputBlockField
        """
        new_inner_output_train = InnerOutputField(
            name="%s_train_i" % new_port.name,
            provided_data_type=new_port.required_data_type
        )
        new_inner_output_test = InnerOutputField(
            name="%s_test_i" % new_port.name,
            provided_data_type=new_port.required_data_type
        )
        self.inner_output_es_names_map[new_port.name] = \
            (new_inner_output_train.name, new_inner_output_test.name)
        self.inner_output_manager.register(new_inner_output_train)
        self.inner_output_manager.register(new_inner_output_test)
        self._block_serializer.register(new_inner_output_train)
        self._block_serializer.register(new_inner_output_test)

        scope = self.get_sub_scope()
        scope.load()
        scope.register_variable(ScopeVar(
            self.uuid, new_inner_output_train.name, new_inner_output_train.provided_data_type))
        scope.register_variable(ScopeVar(
            self.uuid, new_inner_output_test.name, new_inner_output_test.provided_data_type))
        scope.store()

    def get_inner_out_var(self, name):
        return self.inner_output_manager.get_var(name)

    def run_sub_scope(self, exp, *args, **kwargs):
        self.reset_execution_for_sub_blocks()

        exp.store_block(self)
        sr = ScopeRunner(exp, self.sub_scope_name)
        sr.execute()

    def on_sub_scope_done(self, exp, *args, **kwargs):
        """
            @type exp: Experiment

            This action should be called by ScopeRunner
            when all blocks in sub-scope have exec status == done
        """

        r = get_redis_instance()
        with redis_lock.Lock(r, ExpKeys.get_metablock_collect_lock_key(self.exp_id, self.uuid)):
            res_seq = self.get_out_var("res_seq")
            cell = {}
            for name, scope_var in self.collector_spec.bound.iteritems():
                var = exp.get_scope_var_value(scope_var)
                print "Collected %s from %s" % (var, scope_var.title)
                if var is not None:
                    cell[name] = deepcopy(var)

            res_seq.sequence[self.inner_output_manager.iterator] = cell
            self.set_out_var("res_seq", res_seq)
            # print "Storing res_seq on iter %s: %s" % (self.inner_output_manager.iterator, res_seq.to_dict())
            exp.store_block(self)

        # print "Collected fold results: %s " % cell
        if len(cell) < len(res_seq.fields):
            self.do_action("continue_collecting_sub_scope", exp)
        else:
            try:
                self.inner_output_manager.next()
                self.do_action("run_sub_scope", exp)
            except StopIteration, e:
                # All folds was processed without errors
                self.do_action("success", exp)

    def continue_collecting_sub_scope(self, exp, *args, **kwargs):
        pass

    def execute(self, exp, *args, **kwargs):
        self.clean_errors()

        self.inner_output_manager.reset()
        es_dict = {
            inp_name: self.get_input_var(inp_name)
            for inp_name in self.es_inputs
        }

        self.celery_task = wrapper_task.s(
            generate_cv_folds,
            exp, self,
            folds_num=self.folds_num,
            es_dict=es_dict,
            inner_output_es_names_map=self.inner_output_es_names_map,
            success_action="on_folds_generation_success",
        )
        exp.store_block(self)
        self.celery_task.apply_async()

    def on_folds_generation_success(self, exp, sequence, *args, **kwargs):
        self.inner_output_manager.sequence = sequence
        self.inner_output_manager.next()

        res_seq = self.get_out_var("res_seq")
        res_seq.clean_content()
        res_seq.sequence = [None for _ in sequence]
        self.set_out_var("res_seq", res_seq)

        exp.store_block(self)
        self.do_action("run_sub_scope", exp)

    def success(self, exp, *args, **kwargs):
        pass
        # pprint(args)
        # pprint(kwargs)

    def add_collector_var(self, exp, *args, **kwargs):
        super(CrossValidation, self).add_collector_var(exp, *args, **kwargs)
        res_seq = self.get_out_var("res_seq")
        res_seq.fields = {
            name: var.data_type
            for name, var in self.collector_spec.bound.iteritems()
        }
        exp.store_block(self)