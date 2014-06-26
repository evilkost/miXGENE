# -*- coding: utf-8 -*-
from collections import defaultdict
import logging

import numpy as np
from sklearn import decomposition

from mixgene.util import log_timing
from webapp.tasks import wrapper_task
from workflow.blocks.blocks_pallet import GroupType
from workflow.blocks.fields import FieldType, BlockField, InputType, ParamField, ActionsList, ActionRecord, \
    InputBlockField
from workflow.blocks.generic import GenericBlock


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class PcaVisualize(GenericBlock):
    block_base_name = "PCA_VISUALIZE"
    name = "2D PCA Plot"
    block_group = GroupType.VISUALIZE

    is_block_supports_auto_execution = False

    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready", "input_bound"], "validating_params",
                     user_title="Save parameters"),

        ActionRecord("on_params_not_valid", ["validating_params"], "created"),
        ActionRecord("on_params_is_valid", ["validating_params"], "valid_params"),

        ActionRecord("compute_pca", ["valid_params"], "computing_pca", user_title="Compute PCA"),
        ActionRecord("pca_done", ["computing_pca"], "done",),

        ActionRecord("reset_execution", ["*", "done", "execution_error", "ready", "working"], "ready",
                     user_title="Reset execution")

        #ActionRecord("update", ["input_bound", "ready"], "ready"),
    ])

    input_es = InputBlockField(name="es", order_num=10,
        required_data_type="ExpressionSet", required=True)

    chart_series = BlockField(name="chart_series", field_type=FieldType.RAW, init_val=[])
    chart_categories = BlockField(name="chart_categories", field_type=FieldType.SIMPLE_LIST,
                              init_val=[])

    elements = BlockField(name="elements", field_type=FieldType.SIMPLE_LIST, init_val=[
        "pca.html"
    ])

    def __init__(self, *args, **kwargs):
        super(PcaVisualize, self).__init__("PCA visualise", *args, **kwargs)

    def on_params_is_valid(self, exp, *args, **kwargs):
        super(PcaVisualize, self).on_params_is_valid(exp, *args, **kwargs)

        self.do_action("compute_pca", exp)

    def compute_pca(self, exp, *args, **kwargs):
        log.info("compute pca invoked")

        es = self.get_input_var("es")
        """:type :ExpressionSet"""
        df = es.get_assay_data_frame()
        pheno_df = es.get_pheno_data_frame()
        target_column = es.pheno_metadata['user_class_title']

        X = df.as_matrix().transpose()

        pca_model = decomposition.PCA(n_components=2)
        pca_model.fit(X)
        Xp = pca_model.transform(X).tolist()

        names = [x.strip() for x in pheno_df[target_column].tolist()]

        series_by_names = defaultdict(list)
        for x, name in zip(Xp, names):
            series_by_names[name].append(x)

        self.chart_series = [
            {
                "name": name,
                "data": points
            }
            for name, points in series_by_names.iteritems()
        ]
        self.do_action("pca_done", exp)

    def pca_done(self, exp, *args, **kwargs):
        log.info("pca done")

