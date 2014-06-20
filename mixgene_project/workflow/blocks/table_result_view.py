# -*- coding: utf-8 -*-

from collections import defaultdict
import hashlib
import logging

import numpy as np
from sklearn import decomposition

from mixgene.util import log_timing
from webapp.tasks import wrapper_task
from workflow.blocks.fields import FieldType, BlockField, InputType, ParamField, ActionsList, ActionRecord, \
    InputBlockField
from workflow.blocks.generic import GenericBlock


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class TableResultView(GenericBlock):
    block_base_name = "TableResultView"
    is_block_supports_auto_execution = False

    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready", "input_bound"], "validating_params",
                     user_title="Save parameters"),

        ActionRecord("on_params_not_valid", ["validating_params"], "created"),
        ActionRecord("on_params_is_valid", ["validating_params"], "valid_params"),

        #ActionRecord("compute_pca", ["valid_params"], "computing_pca", user_title="Compute PCA"),
        #ActionRecord("pca_done", ["computing_pca"], "done",),

        #ActionRecord("reset_execution", ["*", "done", "execution_error", "ready", "working"], "ready",
        #             user_title="Reset execution")

        #ActionRecord("update", ["input_bound", "ready"], "ready"),
    ])

    input_table_result = InputBlockField(name="tr", order_num=10,
                               required_data_type="TableResult", required=True)

    _table_for_js = BlockField(name="table_js", field_type=FieldType.RAW, is_a_property=True)

    #chart_series = BlockField(name="chart_series", field_type=FieldType.RAW, init_val=[])
    #chart_categories = BlockField(name="chart_categories", field_type=FieldType.SIMPLE_LIST,
    #                              init_val=[])

    elements = BlockField(name="elements", field_type=FieldType.SIMPLE_LIST, init_val=[
        "table_result_view.html"
    ])

    def __init__(self, *args, **kwargs):
        super(TableResultView, self).__init__("PCA visualise", *args, **kwargs)

    @property
    def table_js(self):
        tr = self.get_input_var("tr")
        """:type :TableResult"""
        if tr:
            table = tr.get_table()
            table_headers = ["#"] + table.columns.tolist()

            column_title_to_code_name = {
                title: "_" + hashlib.md5(title).hexdigest()[:8]
                for title in table_headers
            }
            fields_list = [column_title_to_code_name[title] for title in table_headers]

            return {
                "columns": [
                    {
                        "title": title,
                        "field": column_title_to_code_name[title],
                        "visible": True
                    }
                    for title in table_headers
                ],
                "rows": [
                    dict(zip(fields_list, row))
                    for row in
                    table.to_records().tolist() #[:100]
                ]
            }
        else:
            None