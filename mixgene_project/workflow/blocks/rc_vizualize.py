# -*- coding: utf-8 -*-

import logging
import json

from environment.structures import TableResult
from mixgene.util import log_timing
from webapp.tasks import wrapper_task
from workflow.blocks.fields import FieldType, BlockField, OutputBlockField, InputBlockField, InputType, ParamField, \
    ActionRecord, ActionsList
from workflow.blocks.generic import GenericBlock, save_params_actions_list, execute_block_actions_list

from wrappers.scoring import metrics_dict

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class RcVisualizer(GenericBlock):
    block_base_name = "RC_VIZUALIZER"
    is_block_supports_auto_execution = False

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
    _available_metrics = BlockField(name="available_metrics",
                                    field_type=FieldType.RAW,
                                    is_a_property=True)

    metric = ParamField(name="metric", title="Metric", field_type=FieldType.STR,
                        input_type=InputType.SELECT, select_provider="available_metrics")

    def __init__(self, *args, **kwargs):
        super(RcVisualizer, self).__init__(*args, **kwargs)

    @property
    @log_timing
    def available_metrics(self):
        try:
            return [
                {"pk": metric_name, "str": metric.title}
                for metric_name, metric in metrics_dict.iteritems()
                if metric.produce_single_number
            ]
        except Exception, e:
            log.exception(e)
            return []

    @property
    def rc(self):
        return self.get_input_var("results_container")
