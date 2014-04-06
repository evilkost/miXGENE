# -*- coding: utf-8 -*-
import logging

import numpy as np

from mixgene.util import log_timing
from webapp.tasks import wrapper_task
from workflow.blocks.fields import FieldType, BlockField, InputType, ParamField

from workflow.blocks.rc_vizualize import RcVisualizer

from wrappers.boxplot_stats import boxplot_stats

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


def fix_nan(val):
    if val is None:
        return None
    if np.isnan(val):
        return None
    else:
        return float(val)


class BoxPlot(RcVisualizer):
    block_base_name = "BOX_PLOT"

    boxplot_config = ParamField(name="boxplot_config", title="",
                              input_type=InputType.HIDDEN,
                              field_type=FieldType.RAW)

    plot_inputs = BlockField(name="plot_inputs", field_type=FieldType.RAW, init_val=[])
    chart_series = BlockField(name="chart_series", field_type=FieldType.RAW, init_val=[])
    chart_categories = BlockField(name="chart_categories", field_type=FieldType.SIMPLE_LIST,
                                  init_val=[])

    elements = BlockField(name="elements", field_type=FieldType.SIMPLE_LIST, init_val=[
        "box_plot.html"
    ])

    def __init__(self, *args, **kwargs):
        super(BoxPlot, self).__init__("Box plot", *args, **kwargs)
        self.boxplot_config = {
            "agg_axis_for_scoring": {},
            "compare_axis_by_boxplot": {},
        }

    @log_timing
    def compute_boxplot_stats(self, exp, *args, **kwargs):
        agg_axis_for_scoring = [
            axis for axis, is_selected in
            self.boxplot_config["agg_axis_for_scoring"].items() if is_selected
        ]
        compare_axis_by_boxplot = [
            axis for axis, is_selected in
            self.boxplot_config["compare_axis_by_boxplot"].items() if is_selected
        ]
        rc = self.rc

        if compare_axis_by_boxplot and rc:
            rc.load()

            df = rc.get_pandas_slice_for_boxplot(
                compare_axis_by_boxplot,
                agg_axis_for_scoring or [],
                self.metric
            )

            categories = []
            for row_id, _ in df.iterrows():
                if type(row_id) == tuple:
                    title = ":".join(map(str, row_id))
                else:
                    title = str(row_id)

                categories.append(title)

            # import ipdb; ipdb.set_trace()
            bps = boxplot_stats(np.array(df.T, dtype=float))

            if bps:
                self.chart_series = [{
                    "data": [],
                }, {
                    "name": "Outliers",
                    "data": [],
                    "type": "scatter",
                    "marker": {
                        "fillColor": "white",
                        "lineWidth": 1,
                        "lineColor": "blue"
                    },
                    "tooltip": {
                        "pointFormat": '%s: {point.y} ' % self.metric
                    }


                }]
                self.chart_series[0]["data"] = [
                    [
                        fix_nan(rec["whislo"]),
                        fix_nan(rec["q1"]),
                        fix_nan(rec["med"]),
                        fix_nan(rec["q3"]),
                        fix_nan(rec["whishi"])
                    ]
                    for rec in bps
                ]
                for cat_idx, rec in enumerate(bps):
                    for outlier in rec['fliers']:
                        self.chart_series[1]["data"].append([cat_idx, outlier])

                self.chart_categories = categories
                exp.store_block(self)

    def on_params_is_valid(self, exp, *args, **kwargs):
        super(BoxPlot, self).on_params_is_valid(exp, *args, **kwargs)
        if self.rc is not None:
            for axis in self.rc.axis_list:
                if axis not in self.boxplot_config["agg_axis_for_scoring"]:
                    self.boxplot_config["agg_axis_for_scoring"][axis] = ""
                if axis not in self.boxplot_config["compare_axis_by_boxplot"]:
                    self.boxplot_config["compare_axis_by_boxplot"][axis] = ""


            self.compute_boxplot_stats(exp)
        exp.store_block(self)
