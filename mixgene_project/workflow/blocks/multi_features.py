# -*- coding: utf-8 -*-

from copy import deepcopy
import json
from pprint import pprint

import redis_lock
import pandas as pd


from mixgene.redis_helper import ExpKeys
from mixgene.util import get_redis_instance
from webapp.models import Experiment
from environment.structures import SequenceContainer
from webapp.scope import ScopeRunner, ScopeVar
from workflow.common_tasks import generate_cv_folds, wrapper_task
from generic import InnerOutputField
from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, OutputBlockField, InputBlockField, IteratedInnerFieldManager

from environment.structures import prepare_phenotype_for_js_from_es


def prepare_folds(exp, block, features, es_dict, inner_output_es_names_map):
    """
        @type features: list
        @param features: Phenotype features to use as target class

        @type es_dict: dict
        @param es_dict: {input_name -> ExpressionSet}

        @type inner_output_es_names_map: dict
        @param inner_output_es_names_map: input field name -> inner output name
    """
    seq = []

    pheno_df = es_dict.values()[0].get_pheno_data_frame()

    for num, feature in enumerate(features):
        mask = pd.notnull(pheno_df[feature])

        cell = {}
        for input_name, output_name in inner_output_es_names_map.iteritems():
            es = es_dict[input_name]
            modified_es = es.clone(
                base_filename="%s_%s_%s" % (block.uuid, input_name, num),

            )
            modified_pheno_df = pheno_df[mask]

            modified_es.pheno_metadata["user_class_title"] = feature
            modified_es.store_pheno_data_frame(modified_pheno_df)

            assay_df = es.get_assay_data_frame()
            modified_assay_df = assay_df[assay_df.columns[mask]]
            modified_es.store_assay_data_frame(modified_assay_df)

            cell[output_name] = modified_es
        seq.append(cell)

    return seq


class MultiFeature(GenericBlock):
    block_base_name = "MULTI_FEATURE"
    create_new_scope = True
    is_block_supports_auto_execution = True

    _block_actions = ActionsList([])
    _block_actions.extend(ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "valid_params"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),

        ActionRecord("on_feature_selection_updated", ["valid_params", "ready", "done"], "ready"),

        ActionRecord("execute", ["ready"], "generating_folds", user_title="Run block"),

        ActionRecord("on_folds_generation_success", ["generating_folds"], "ready_to_run_sub_scope", reload_block_in_client=True),
        ActionRecord("continue_collecting_sub_scope", ["ready_to_run_sub_scope"],
                                          "sub_scope_executing"),

        ActionRecord("run_sub_scope", ["ready_to_run_sub_scope"], "sub_scope_executing"),
        ActionRecord("on_sub_scope_done", ["sub_scope_executing"], "ready_to_run_sub_scope"),

        ActionRecord("success", ["working", "ready_to_run_sub_scope"], "done",
                     propagate_auto_execution=True, reload_block_in_client=True),
        ActionRecord("error", ["ready", "working", "sub_scope_executing",
                               "generating_folds", "ready_to_run_sub_scope"],
                     "execution_error", reload_block_in_client=True),

        ActionRecord("reset_execution", ["*", "done", "sub_scope_executing", "ready_to_run_sub_scope",
                                         "generating_folds", "execution_error"], "ready",
                     user_title="Reset execution"),
    ]))

    _input_es_dyn = InputBlockField(
        name="es_inputs", required_data_type="ExpressionSet",
        required=True, multiply_extensible=True
    )

    _is_sub_pages_visible = BlockField(
        "is_sub_pages_visible", FieldType.RAW,
        init_val=False, is_a_property=True
    )

    pages = BlockField("pages", FieldType.RAW, init_val={
        "select_feature": {
            "title": "Select features to examine",
            "resource": "select_feature",
            "widget": "widgets/select_feature.html"
        },
    })

    _res_seq = OutputBlockField(name="res_seq", provided_data_type="SequenceContainer",
                                   field_type=FieldType.CUSTOM, init_val=SequenceContainer())

    def __init__(self, *args, **kwargs):
        super(MultiFeature, self).__init__("Multi feature block", *args, **kwargs)

        self.inner_output_manager = IteratedInnerFieldManager()
        self.features = []
        self.inner_output_es_names_map = {}
        self.celery_task = None

        self.set_out_var("res_seq", SequenceContainer())

    @property
    def is_sub_pages_visible(self):
        if self.state in ['valid_params', 'done', 'ready']:
            return True
        return False

    def add_dyn_input_hook(self, exp, dyn_port, new_port):
        """
            @type new_port: InputBlockField
        """
        new_inner_output = InnerOutputField(
            name="%s_i" % new_port.name,
            provided_data_type=new_port.required_data_type
        )
        self.inner_output_es_names_map[new_port.name] = new_inner_output.name
        self.inner_output_manager.register(new_inner_output)
        self._block_serializer.register(new_inner_output)

        scope = self.get_sub_scope()
        scope.load()
        scope.register_variable(ScopeVar(
            self.uuid, new_inner_output.name, new_inner_output.provided_data_type))
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

    def execute(self, exp, *arga, **kwargs):
        # self.celery_task = wrapper_task.s(
        #
        # )
        self.inner_output_manager.reset()
        es_dict = {
            inp_name: self.get_input_var(inp_name)
            for inp_name in self.es_inputs
        }
        seq = prepare_folds(
            exp, self,
            self.features, es_dict,
            self.inner_output_es_names_map
        )

        exp.store_block(self)
        self.do_action("on_folds_generation_success", exp, seq)

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


    def phenotype_for_js(self, exp, *args, **kwargs):
        es = None
        for input_name in self.es_inputs:
            es = self.get_input_var(input_name)
            if es is not None:
                break
        res = prepare_phenotype_for_js_from_es(es)
        res["features"] = self.features
        return res

    def update_feature_selection(self, exp, request, *args, **kwargs):
        pprint(request.body)
        req = json.loads(request.body)
        self.features = req["features"]
        if self.features:
            self.do_action("on_feature_selection_updated", exp)

    def on_feature_selection_updated(self, *args, **kwargs):
        pass

    def add_collector_var(self, exp, *args, **kwargs):
        super(MultiFeature, self).add_collector_var(exp, *args, **kwargs)
        res_seq = self.get_out_var("res_seq")
        res_seq.fields = {
            name: var.data_type
            for name, var in self.collector_spec.bound.iteritems()
        }
        exp.store_block(self)


