from django import forms
from webapp.models import BroadInstituteGeneSet

from workflow.ports import BlockPort


from generic import GenericBlock

import json

import pandas as pd


from webapp.models import Experiment
from workflow.common_tasks import fetch_geo_gse, preprocess_soft
from workflow.execution import ExecStatus

from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, execute_block_actions_list, OutputBlockField, InputBlockField


# class GeneSetSelectionForm(forms.Form):
#     msigdb_id = forms.IntegerField()
#
#     def clean_msigdb_id(self):
#         data = self.cleaned_data["msigdb_id"]
#         if len(BroadInstituteGeneSet.objects.filter(id=data)) == 0:
#             raise forms.ValidationError("Got wrong gene set identifier, try again")


class GetBroadInstituteGeneSet(GenericBlock):
    block_base_name = "BI_GENE_SET"
    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "done"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),
    ])

    # TODO: maybe create more general solution ?
    _all_gene_sets = BlockField("all_gene_sets", title="", input_type=InputType.HIDDEN,
                               field_type=FieldType.RAW)

    msigdb_id = ParamField(name="msigdb_id", title="MSigDB gene set", input_type=InputType.SELECT,
                           field_type=FieldType.INT, init_val=0,  # TODO: fix hardcoded value
                           select_provider="all_gene_sets")

    _gs = OutputBlockField(name="gs", field_type=FieldType.HIDDEN,
                           provided_data_type="GeneSets")

    @property
    def all_gene_sets(self):
        return BroadInstituteGeneSet.get_all_meta()

    def __init__(self, *args, **kwargs):
        super(GetBroadInstituteGeneSet, self).__init__("Get MSigDB gene set", *args, **kwargs)
        # self.gene_sets = None
        # self.errors = []
        # self.selected_gs_id = None

    def on_params_is_valid(self, exp):
        gs = BroadInstituteGeneSet.objects.get(pk=self.msigdb_id).get_gene_sets()
        self.set_out_var("gs", gs)

        super(GetBroadInstituteGeneSet, self).on_params_is_valid(exp)


    # def serialize(self, exp, to="dict"):
    #     hash = super(GetBroadInstituteGeneSet, self).serialize(exp, to)
    #     hash["all_gene_sets"] = BroadInstituteGeneSet.get_all_meta()
    #     return hash