# -*- coding: utf-8 -*-
from pprint import pprint
import json

from environment.structures import TableResult
from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, execute_block_actions_list, OutputBlockField, InputBlockField


from wrappers.boxplot_stats import boxplot_stats
from workflow.common_tasks import wrapper_task
from wrappers.gt import global_test_task


class BoxPlot(GenericBlock):
    block_base_name = "BOX_PLOT"
    is_block_supports_auto_execution = True

    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready", "input_bound"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "input_bound"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),

        ActionRecord("configure_plot", ["input_bound", "ready"], "ready"),
    ])
    _block_actions.extend(execute_block_actions_list)

    _res_seq = InputBlockField(name="res_seq", required_data_type="SequenceContainer", required=True)

    _res_seq_for_js = BlockField("res_seq_for_js", field_type=FieldType.RAW, is_a_property=True)
    plot_options = BlockField(name="plot_options", field_type=FieldType.SIMPLE_DICT, init_val={})
    elements = BlockField(name="elements", field_type=FieldType.SIMPLE_LIST, init_val=[
        "box_plot.html"
    ])

    def __init__(self, *args, **kwargs):
        super(BoxPlot, self).__init__("Box plot", *args, **kwargs)

        self.is_block_supports_auto_execution = True
        self.celery_task = None

    def compute_boxplot_stats(self, exp, request, *args, **kwargs):
        plot_inputs = json.loads(request.body)["plot_inputs"]
        X = []

        seq = self.get_input_var("res_seq").sequence

        for input_def in plot_inputs:
            series = []
            input_name = input_def["name"]
            metric  = input_def["metric"]

            X.append([cell[input_name].scores[metric] for cell in seq])

        return boxplot_stats(X)


    @property
    def res_seq_for_js(self):
        res_seq = self.get_input_var("res_seq")
        if res_seq is None:
            return {}

        pprint(res_seq.to_dict())
        return res_seq.to_dict()

    def execute(self, exp, *args, **kwargs):
        self.clean_errors()
        # self.celery_task = wrapper_task.s(
        #     global_test_task,
        #     exp, self,
        #     es=self.get_input_var("es"),
        #     gene_sets=self.get_input_var("gs"),
        #     base_dir=exp.get_data_folder(),
        #     base_filename="%s_gt_result" % self.uuid,
        #     )
        exp.store_block(self)
        # self.celery_task.apply_async()

    def success(self, exp, result, *args, **kwargs):
        pass