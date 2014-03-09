# -*- coding: utf-8 -*-
import logging
from pprint import pprint
import json

from environment.structures import TableResult
from webapp.tasks import wrapper_task
from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, execute_block_actions_list, OutputBlockField, InputBlockField


from wrappers.boxplot_stats import boxplot_stats
from wrappers.gt import global_test_task

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

class BoxPlot(GenericBlock):
    block_base_name = "BOX_PLOT"
    # is_block_supports_auto_execution = False

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

    plot_inputs = BlockField(name="plot_inputs", field_type=FieldType.RAW, init_val=[])
    chart_series = BlockField(name="chart_series", field_type=FieldType.RAW,
                              init_val=[{"data": [], "name": "ML scores"}])
    chart_categories = BlockField(name="chart_categories", field_type=FieldType.SIMPLE_LIST,
                                  init_val=[])


    elements = BlockField(name="elements", field_type=FieldType.SIMPLE_LIST, init_val=[
        "box_plot.html"
    ])

    def __init__(self, *args, **kwargs):
        super(BoxPlot, self).__init__("Box plot", *args, **kwargs)

        self.is_block_supports_auto_execution = True
        self.celery_task = None

    def compute_boxplot_stats(self, exp, request, *args, **kwargs):
        self.plot_inputs = json.loads(request.body)["plot_inputs"]
        X = []
        seq = self.get_input_var("res_seq").sequence

        categories = []
        for input_def in self.plot_inputs:

            input_name = input_def["name"]
            metric  = input_def["metric"]

            categories.append("%s:%s" % (input_name, metric))

            X.append([cell[input_name].scores[metric] for cell in seq])

        # return boxplot_stats(X)
        bps = boxplot_stats(X)
        self.chart_series[0]["data"] = [
             [
                rec["whislo"],
                rec["q1"],
                rec["med"],
                rec["q3"],
                rec["whishi"]
             ]
             for rec in bps
        ]
        self.chart_categories = categories
        log.debug("stored chart series: %s", self.chart_series)
        exp.store_block(self)


    @property
    def res_seq_for_js(self):
        res_seq = self.get_input_var("res_seq")
        if res_seq is None:
            return {}

        pprint(res_seq.to_dict())
        return res_seq.to_dict()

    # def execute(self, exp, *args, **kwargs):
    #     self.reset(exp)
    #     self.do_action("success", exp)
    #
    # def reset_execution(self, exp, *args, **kwargs):
    #     self.reset(exp)
    #     pass
    #
    # def reset(self, exp):
    #     self.clean_errors()
    #     self.plot_inputs = []
    #     self.chart_categories = []
    #     self.chart_series = self._block_serializer.fields["chart_series"].init_val

    def success(self, exp, result, *args, **kwargs):
        pass