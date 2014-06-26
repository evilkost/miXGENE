# -*- coding: utf-8 -*-
import logging
import cStringIO as StringIO

from django.core.urlresolvers import reverse
from workflow.blocks.fields import FieldType, BlockField, InputType, ParamField

from workflow.blocks.rc_vizualize import RcVisualizer


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


pd_float_format_func = lambda x: "%1.4f" % x

class TableObj(object):
    def __init__(self):
        self.html = ""
        self.df = None

    def to_dict(self, *args, **kwargs):
        return {
            "html": self.html
        }


class RenderTable(RcVisualizer):
    block_base_name = "RENDER_TABLE"
    name = "Results container as table"

    _table = BlockField(name="table", field_type=FieldType.CUSTOM, is_a_property=True)
    _export_table_url = BlockField(name="export_table_url",
                                   field_type=FieldType.STR, is_a_property=True)
    _export_raw_results_url = BlockField(name="export_raw_results_url",
                                   field_type=FieldType.STR, is_a_property=True)

    elements = BlockField(name="elements", field_type=FieldType.SIMPLE_LIST, init_val=[
        "rc_table.html"
    ])

    table_config = ParamField(name="table_config", title="",
                              input_type=InputType.HIDDEN,
                              field_type=FieldType.RAW)

    def __init__(self, *args, **kwargs):
        super(RenderTable, self).__init__(*args, **kwargs)
        self.table_config = {
            "header_axis": "",
            "multi_index_axis_dict": {},
        }

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
                # log.debug("Can build table slice")

                df = rc.get_pandas_slice(header_axis, index_axis_list,
                                         metric_name=self.metric)
                # log.debug(df)
                to.html = df.to_html(float_format=pd_float_format_func)
                to.df = df
            else:
                log.debug("Can't build table slice, header axis `%s`, index axis_list `%s`",
                          header_axis, index_axis_list)

            # log.debug("Table: %s", to.to_dict())
        return to

    @property
    def export_table_url(self):
        return reverse("block_field_formatted", kwargs={
            "exp_id": self.exp_id,
            "block_uuid": self.uuid,
            "field": "export_table",
            "format": "csv"
        })

    @property
    def export_raw_results_url(self):
        return reverse("block_field_formatted", kwargs={
            "exp_id": self.exp_id,
            "block_uuid": self.uuid,
            "field": "export_rc",
            "format": "json"
        })
        # import ipdb; ipdb.set_trace()
        # return

    def export_rc(self, exp, *args, **kwargs):
        return self.rc.export_to_json_dict()

    def export_table(self, exp, *args, **kwargs):
        table = self.table
        out = StringIO.StringIO()
        # Float format in fact doesn't work in pandas
        # table.df.to_csv(out, float_format=pd_float_format_func)
        #
        tmp_df = table.df.applymap(pd_float_format_func)
        tmp_df.to_csv(out, float_format=pd_float_format_func)

        out.seek(0)
        return out.read()

    def on_params_is_valid(self, exp, *args, **kwargs):
        super(RenderTable, self).on_params_is_valid(exp, *args, **kwargs)
        if self.rc is not None:
            for axis in self.rc.axis_list:
                if axis not in self.table_config["multi_index_axis_dict"]:
                    self.table_config["multi_index_axis_dict"][axis] = ""
        exp.store_block(self)
