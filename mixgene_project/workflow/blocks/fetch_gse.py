__author__ = 'kost'

from collections import defaultdict
import json

from django import forms
from fysom import Fysom
import pandas as pd

from workflow.common_tasks import fetch_geo_gse, preprocess_soft

from generic import GenericBlock


class FetchGseForm(forms.Form):
    geo_uid = forms.CharField(min_length=4, max_length=31, required=True)

    def clean_geo_uid(self):
        data = self.cleaned_data['geo_uid']
        if data[:3].upper() != 'GSE' not in data:
            raise forms.ValidationError("Geo uid should have 'GSE' prefix")
        return data


class FetchGSE(GenericBlock):
    fsm = Fysom({
        'events': [
            {'name': 'save_form', 'src': 'created', 'dst': 'form_modified'},
            {'name': 'save_form', 'src': 'form_modified', 'dst': 'form_modified'},

            {'name': 'on_form_is_valid', 'src': 'form_modified', 'dst': 'form_valid'},
            {'name': 'on_form_not_valid', 'src': 'form_modified', 'dst': 'form_modified'},

            {'name': 'reset_form', 'src': 'form_modified', 'dst': 'created'},
            {'name': 'reset_form', 'src': 'form_valid', 'dst': 'created'},

            {'name': 'show_form', 'src': 'form_valid', 'dst': 'form_modified'},

            {'name': 'start_fetch', 'src': 'form_valid', 'dst': 'source_is_being_fetched'},

            {'name': 'error_during_fetch', 'src': 'source_is_being_fetched', 'dst': 'form_valid'},
            {'name': 'successful_fetch', 'src': 'source_is_being_fetched', 'dst': 'source_was_fetched'},

            {'name': 'start_preprocess', 'src': 'source_was_fetched', 'dst': 'source_is_being_preprocessed'},

            {'name': 'error_during_preprocess', 'src': 'source_is_being_preprocessed', 'dst': 'form_valid'},
            {'name': 'successful_preprocess', 'src': 'source_is_being_preprocessed', 'dst': 'source_was_preprocessed'},

            {'name': 'assign_sample_classes', 'src': 'source_was_preprocessed', 'dst': 'sample_classes_assigned'},
            {'name': 'assign_sample_classes', 'src': 'sample_classes_assigned', 'dst': 'sample_classes_assigned'},
            ],
        })

    all_actions = [
        # method name, human readable title, user visible
        ("save_form", "Save parameters", True),

        ("on_form_is_valid", "", False),
        ("on_form_not_valid", "", False),

        ("show_form", "Edit parameters", True),
        ("reset_form", "Reset parameters", True),

        ("start_fetch", "Fetch data", True),
        ("error_during_fetch", "", False),
        ("successful_fetch", "", False),

        ("start_preprocess", "", False),
        ("error_during_preprocess", "", False),
        ("successful_preprocess", "", False),

        ("assign_sample_classes", "", False),
        ]
    # widget = "widgets/fetch_ncbi_gse.html"
    widget = "widgets/fetch_gse/assign_sample_classes.html"
    pages = {
        "assign_sample_classes": {
            "title": "Assign sample classes",
            "resource": "assign_sample_classes",
            #"widget": "static/templates/fetch_gse/assign_sample_classes.html"
            "widget": "widgets/fetch_gse/assign_sample_classes.html"

        },
        }
    form_cls = FetchGseForm
    form_data = {
        # "expression_set_name": "expression",
        # "gpl_annotation_name": "annotation",
    }
    block_base_name = "FETCH_GEO"
    is_base_name_visible = True

    provided_objects = {
        "expression_set": "ExpressionSet",
        "gpl_annotation": "PlatformAnnotation",
        }
    #TODO: param proto class
    params_prototype = {
        "geo_uid": {
            "name": "geo_uid",
            "title": "Geo accession id",
            "input_type": "text",
            "validation": None,
            "default": "",
            }
    }

    def __init__(self, *args, **kwargs):
        super(FetchGSE, self).__init__("Fetch ncbi gse", *args, **kwargs)

        self.form = self.form_cls(self.form_data)

        self.source_file = None

        self.celery_task_fetch = None
        self.celery_task_preprocess = None

        self.errors = []

        self.expression_set = None
        self.gpl_annotation = None

    def get_expression_set_name(self):
        return self.form["expression_set_name"].value()

    def get_gse_source_name(self):
        return "%s_source" % self.uuid

    def get_geo_uid(self):
        return self.form["geo_uid"].value()

    def get_annotation_name(self):
        return self.form["gpl_annotation_name"].value()

    def is_form_fields_editable(self):
        if self.state in ['created', 'form_modified']:
            return True
        return False

    @property
    def is_sub_pages_visible(self):
        if self.state in ['source_was_preprocessed', 'sample_classes_assigned']:
            return True
        return False

    def show_form(self, exp, *args, **kwargs):
        exp.store_block(self)

    def reset_form(self, exp, *args, **kwargs):
        self.clean_errors()
        self.form = self.form_cls(self.form_data)
        self.is_form_visible = True
        self.file_can_be_fetched = False
        exp.store_block(self)

    def start_fetch(self, exp, *args, **kwargs):
        self.clean_errors()
        self.celery_task_fetch = fetch_geo_gse.s(exp, self, ignore_cache=False).apply_async()
        exp.store_block(self)

    def error_during_fetch(self, exp, *args, **kwargs):
        exp.store_block(self)

    def successful_fetch(self, exp, *args, **kwargs):
        self.clean_errors()
        self.do_action("start_preprocess", exp)
        exp.store_block(self)

    def start_preprocess(self, exp, *args, **kwargs):
        self.celery_task_preprocess = preprocess_soft.s(exp, self).apply_async()
        exp.store_block(self)

    def error_during_preprocess(self, exp, *args, **kwargs):
        exp.store_block(self)

    def successful_preprocess(self, exp, *args, **kwargs):
        self.clean_errors()
        exp.store_block(self)

    def assign_sample_classes(self, exp, request, *args, **kwargs):
        # Shift to celery
        pheno_df = self.expression_set.get_pheno_data_frame()
        sample_classes = json.loads(request.POST['sample_classes'])
        pheno_df['User_class'] = pd.Series(sample_classes)

        self.expression_set.store_pheno_data_frame(pheno_df)
        exp.store_block(self)

    def revoke_task(self, exp, *args, **kwargs):
        pass
        #self.celery_task_fetch.revoke(terminate=True)
        #exp.store_block(self)