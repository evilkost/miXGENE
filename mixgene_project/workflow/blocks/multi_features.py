# -*- coding: utf-8 -*-

from copy import deepcopy
import json
import logging

import redis_lock
import pandas as pd


from mixgene.redis_helper import ExpKeys
from mixgene.util import get_redis_instance
from webapp.models import Experiment
from environment.structures import SequenceContainer
from webapp.scope import ScopeRunner, ScopeVar
from webapp.tasks import wrapper_task
from workflow.blocks.fields import FieldType, BlockField, OutputBlockField, InnerOutputField, InputBlockField, InputType, \
    ParamField, ActionRecord, ActionsList
from workflow.blocks.managers import IteratedInnerFieldManager
from workflow.blocks.meta_block import UniformMetaBlock
from workflow.common_tasks import generate_cv_folds
from workflow.blocks.generic import GenericBlock, save_params_actions_list

from environment.structures import prepare_phenotype_for_js_from_es

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

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
            # Reorder columns to be compatible to phenotype
            assay_df = assay_df[pheno_df.index]

            modified_assay_df = assay_df[assay_df.columns[mask]]
            modified_es.store_assay_data_frame(modified_assay_df)

            cell[output_name] = modified_es
        seq.append(cell)

    return seq


class MultiFeature(UniformMetaBlock):
    block_base_name = "MULTI_FEATURE"

    _mf_block_actions = ActionsList([
        ActionRecord("on_feature_selection_updated", ["valid_params", "ready", "done"], "ready"),
    ])

    _input_es_dyn = InputBlockField(
        name="es_inputs", order_num=-10,
        required_data_type="ExpressionSet",
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

    def __init__(self, *args, **kwargs):
        super(MultiFeature, self).__init__("Multi feature block", *args, **kwargs)
        self.features = []

    @property
    def is_sub_pages_visible(self):
        if self.state in ['valid_params', 'done', 'ready']:
            return True
        return False

    def get_fold_labels(self):
        return self.features

    def add_dyn_input_hook(self, exp, dyn_port, new_port):
        """
            @type new_port: InputBlockField
        """
        new_inner_output = InnerOutputField(
            name="%s_i" % new_port.name,
            provided_data_type=new_port.required_data_type
        )
        self.inner_output_es_names_map[new_port.name] = new_inner_output.name
        self.register_inner_output_variables([new_inner_output])

    def execute(self, exp, *args, **kwargs):
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
        req = json.loads(request.body)
        self.features = req["features"]
        if self.features:
            self.do_action("on_feature_selection_updated", exp)

    def on_feature_selection_updated(self, *args, **kwargs):
        pass
