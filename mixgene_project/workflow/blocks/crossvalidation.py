from collections import defaultdict
from copy import deepcopy
import json
from pprint import pprint

from webapp.models import Experiment
from environment.structures import SequenceContainer
from webapp.scope import ScopeRunner

from workflow.common_tasks import generate_cv_folds

from generic import InnerOutputField

from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, execute_block_actions_list, OutputBlockField, InputBlockField

from converters.gene_set_tools import merge_gs_with_platform_annotation


# class CrossValidationForm(forms.Form):
#     folds_num = forms.IntegerField(min_value=2, max_value=100)
#     #split_ratio = forms.FloatField(min_value=0, max_value=1)
#


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


class CrossValidation(GenericBlock):
    block_base_name = "CROSS_VALID"
    create_new_scope = True

    _block_actions = ActionsList([])
    _block_actions.extend(save_params_actions_list)

    _block_actions.extend(ActionsList([
        ActionRecord("execute", ["ready"], "generating_folds", user_title="Run block"),

        ActionRecord("on_folds_generation_success", ["generating_folds"], "ready_to_run_sub_scope"),

        ActionRecord("run_sub_scope", ["ready_to_run_sub_scope"], "sub_scope_executing"),
        ActionRecord("on_sub_scope_done", ["sub_scope_executing"], "ready_to_run_sub_scope"),

        ActionRecord("success", ["working", "ready_to_run_sub_scope"], "done"),
        ActionRecord("error", ["ready", "working", "sub_scope_executing", "generating_folds"], "execution_error"),

        ActionRecord("reset_execution", ['done', "sub_scope_executing",
                                         "generating_folds", "execution_error"], "ready",
                     user_title="Reset execution"),
    ]))

    elements = BlockField(name="elements", field_type=FieldType.SIMPLE_LIST, init_val=[
        "cv_info.html"
    ])

    folds_num = ParamField(name="folds_num", title="Folds number",
                           input_type=InputType.TEXT, field_type=FieldType.INT, init_val=10)
    _input_es = InputBlockField(name="es",
                                required_data_type="ExpressionSet", required=True)

    _es_train_i = InnerOutputField(name="es_train_i", provided_data_type="ExpressionSet")
    _es_test_i = InnerOutputField(name="es_test_i", provided_data_type="ExpressionSet")

    _cv_res_seq = OutputBlockField(name="cv_res_seq", provided_data_type="SequenceContainer",
                                   field_type=FieldType.CUSTOM)

    def __init__(self, *args, **kwargs):
        super(CrossValidation, self).__init__("Cross Validation", *args, **kwargs)
        self.auto_exec_status_working.update(["sub_scope_executing", "ready_to_run_sub_scope",
                                              "generating_folds"])

        self.celery_task = None

        self.inner_output_manager = IteratedInnerFieldManager()
        for f_name, f in self._block_serializer.inner_outputs.iteritems():
            self.inner_output_manager.register(f)

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
        cv_res_seq = self.get_out_var("cv_res_seq")
        cell = {}
        for name, scope_var in self.collector_spec.bound.iteritems():
            cell[name] = deepcopy(exp.get_scope_var_value(scope_var))

        cv_res_seq.append(cell)
        self.set_out_var("cv_res_seq", cv_res_seq)
        exp.store_block(self)

        print "Collected fold results: "
        pprint(cell)

        try:
            self.inner_output_manager.next()
            self.do_action("run_sub_scope", exp)
        except StopIteration, e:
            # All folds was processed without errors
            self.do_action("success", exp)

    def execute(self, exp, *args, **kwargs):
        self.clean_errors()

        es = self.get_input_var("es")
        self.inner_output_manager.reset()

        self.celery_task = generate_cv_folds.s(
            exp, self,
            self.folds_num, es,
            success_action="on_folds_generation_success",
        )
        exp.store_block(self)
        self.celery_task.apply_async()

    def on_folds_generation_success(self, exp, sequence, *args, **kwargs):
        self.inner_output_manager.sequence = sequence
        self.inner_output_manager.next()

        cv_res_seq = SequenceContainer()
        cv_res_seq.fields = self.collector_spec.bound.keys()
        self.set_out_var("cv_res_seq", cv_res_seq)

        exp.store_block(self)
        self.do_action("run_sub_scope", exp)

    def success(self, exp, *args, **kwargs):
        pprint(args)
        pprint(kwargs)
