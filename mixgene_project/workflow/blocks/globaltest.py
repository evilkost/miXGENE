from fysom import Fysom

from workflow.ports import BlockPort

from wrappers.gt import global_test_task

from generic import GenericBlock

import json

import pandas as pd


from webapp.models import Experiment
from workflow.common_tasks import fetch_geo_gse, preprocess_soft
from workflow.execution import ExecStatus

from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, execute_block_actions_list, OutputBlockField, InputBlockField


class GlobalTest(GenericBlock):
    block_base_name = "GLOBAL_TEST"

    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "ready"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),
    ])
    _block_actions.extend(execute_block_actions_list)

    _input_es = InputBlockField(name="es", required_data_type="ExpressionSet", required=True)
    _input_gs = InputBlockField(name="gs", required_data_type="GeneSets", required=True)

    elements = [
        "gt_result.html"
    ]

    def __init__(self, *args, **kwargs):
        super(GlobalTest, self).__init__("Global test", *args, **kwargs)

        self.celery_task = None
        self.result = None # TODO: move to output variables

    def gt_result_in_dict(self):
        return ""
        # df = self.pca_result.get_pca()
        # return df.to_json(orient="split")

    def execute(self, exp, request, *args, **kwargs):
        self.clean_errors()
        self.celery_task = global_test_task.s(
            exp, self,
            self.get_input_var("es"), self.get_input_var("gs"),
            exp.get_data_folder(), "%s_gt_result" % self.uuid,
            #exp.get_data_file_path("%s_gt_result" % self.uuid, "csv.gz")
        )
        exp.store_block(self)
        self.celery_task.apply_async()

    def success(self, exp, result):
        self.result = result
        exp.store_block(self)

