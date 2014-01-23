

from collections import defaultdict
import json

from django import forms
from fysom import Fysom
import pandas as pd

from webapp.models import BroadInstituteGeneSet

from generic import GenericBlock


class GeneSetSelectionForm(forms.Form):
    msigdb_id = forms.IntegerField()

    def clean_msigdb_id(self):
        data = self.cleaned_data["msigdb_id"]
        if len(BroadInstituteGeneSet.objects.filter(id=data)) == 0:
            raise forms.ValidationError("Got wrong gene set identifier, try again")


class GetBroadInstituteGeneSet(GenericBlock):
    fsm = Fysom({
        'events': [
            {'name': 'save_form', 'src': 'created', 'dst': 'form_modified'},
            {'name': 'save_form', 'src': 'form_modified', 'dst': 'form_modified'},
            {'name': 'save_form', 'src': 'form_valid', 'dst': 'form_modified'},

            {'name': 'on_form_is_valid', 'src': 'form_modified', 'dst': 'form_valid'},
            {'name': 'on_form_not_valid', 'src': 'form_modified', 'dst': 'form_modified'},
    ]})

    all_actions = [
        # method name, human readable title, user visible
        ("save_form", "Select", True),

        ("on_form_is_valid", "", False),
        ("on_form_not_valid", "", False),
    ]

    widget = "widgets/get_bi_gene_set.html"
    form_cls = GeneSetSelectionForm
    block_base_name = "BI_GENE_SET"
    is_base_name_visible = True

    provided_objects = {
        "gmt": "GmtStorage",
    }

    params_prototype = {
        "msigdb_id": {
            "name": "msigdb_id",
            "title": "Gene set",
            "input_type": "select",
            "validation": None,
            "default": "",
            "data_source": "all_gene_sets",
            "required": True,
        }
    }

    def __init__(self, *args, **kwargs):
        super(GetBroadInstituteGeneSet, self).__init__("Get MSigDB gene set", *args, **kwargs)
        self.gmt = None
        self.errors = []
        self.form = None
        self.selected_gs_id = None

    def on_form_is_valid(self):
        self.errors = []
        self.selected_gs_id = int(self.params["msigdb_id"])
        self.gmt = BroadInstituteGeneSet.objects.get(pk=self.selected_gs_id).get_gmt_storage()
        #print self.selected_gs_id

    def on_form_not_valid(self):
        pass

    def serialize(self, exp, to="dict"):
        hash = super(GetBroadInstituteGeneSet, self).serialize(exp, to)
        hash["all_gene_sets"] = BroadInstituteGeneSet.get_all_meta()
        return hash