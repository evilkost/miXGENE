# -*- coding: utf-8 -*-
import logging
import json

from environment.structures import TableResult
from webapp.tasks import wrapper_task
from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, execute_block_actions_list, OutputBlockField, InputBlockField


from wrappers.boxplot_stats import boxplot_stats
from wrappers.gt import global_test_task
from wrappers.scoring import metrics

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class TableObj(object):
    def __init__(self):
        self.html = ""

    def to_dict(self, *args, **kwargs):
        return {
            "html": self.html
        }


class RenderTable(GenericBlock):
    block_base_name = "RENDER_TABLE"


    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready", "input_bound"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "input_bound"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),

        ActionRecord("configure_table", ["input_bound", "ready"], "ready"),
    ])

    results_container = InputBlockField(name="results_container",
                                       required_data_type="ResultsContainer",
                                       required=True,
                                       field_type=FieldType.CUSTOM)
    _rc = BlockField(name="rc", field_type=FieldType.CUSTOM, is_a_property=True)
    _table = BlockField(name="table", field_type=FieldType.CUSTOM, is_a_property=True)

    elements = BlockField(name="elements", field_type=FieldType.SIMPLE_LIST, init_val=[
        "rc_table.html"
    ])

    _available_metrics = BlockField(name="available_metrics",
                                    field_type=FieldType.RAW,
                                    is_a_property=True)

    metric = ParamField(name="metric", title="Metric", field_type=FieldType.STR,
                        input_type=InputType.SELECT, select_provider="available_metrics")
    table_config = ParamField(name="table_config", title="",
                              input_type=InputType.HIDDEN,
                              field_type=FieldType.RAW)

    def __init__(self, *args, **kwargs):
        super(RenderTable, self).__init__("Result table", *args, **kwargs)
        self.table_config = {
            "header_axis": "",
            "multi_index_axis_dict": {},
        }

    @property
    def available_metrics(self):
        return [
            {"pk": x, "str": x} for x in
            [
                metric.name
                for metric in metrics
                if not metric.require_binary
            ]
        ]

    @property
    def rc(self):
        return self.get_input_var("results_container")

    @property
    def table(self):
        rc = self.rc

        to = TableObj()
        if rc:
            rc.load()
            header_axis = self.table_config.get("header_axis")
            index_axis_list = []
            for axis, flag in self.table_config.get("multi_index_axis_dict", {}).iteritems():
                if flag:
                    index_axis_list.append(axis)

            if header_axis and index_axis_list and hasattr(self, "metric"):
                log.debug("Can build table slice")

                df = rc.get_pandas_slice(header_axis, index_axis_list, metric=self.metric)
                log.debug(df)
                to.html = df.to_html()
            else:
                log.debug("Can't build table slice, header axis `%s`, index axis_list `%s`",
                          header_axis, index_axis_list)

            log.debug("Table: %s", to.to_dict())
        return to

    def on_params_is_valid(self, exp, *args, **kwargs):
        super(RenderTable, self).on_params_is_valid(exp, *args, **kwargs)
        if self.rc is not None:
            for axis in self.rc.axis_list:
                if axis not in self.table_config["multi_index_axis_dict"]:
                    self.table_config["multi_index_axis_dict"][axis] = ""
        exp.store_block(self)