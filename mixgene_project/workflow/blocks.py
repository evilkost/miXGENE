import json
from uuid import uuid1

from django import forms
from fysom import Fysom
import  pandas as pd

from structures import ExpressionSet, PlatformAnnotation
from webapp.models import Experiment

from workflow.common_tasks import fetch_geo_gse, preprocess_soft, append_error_to_block


class GenericBlock(object):
    def __init__(self, name, type):
        """
            Building block for workflow
            @type can be: "user_input", "computation"
        """
        self.uuid = uuid1().hex
        self.name = name
        self.type = type

        # pairs of (var name, data type, default name in context)
        self.required_inputs = []
        self.provide_outputs = []

        self.state = None

        self.errors = []
        self.warnings = []

    def get_available_user_action(self):
        return self.get_allowed_actions(True)

    def get_allowed_actions(self, only_user_actions=False):
        action_list = []
        for line in self.all_actions:
            # action_code, action_title, user_visible = line

            action_code = line[0]
            user_visible = line[2]
            self.fsm.current = self.state

            if self.fsm.can(action_code) and \
                    (not only_user_actions or user_visible):
                action_list.append(line)
        return action_list

    def do_action(self, action_name, *args, **kwargs):
        #TODO: add notification to html client
        if action_name in [row[0] for row in self.get_allowed_actions()]:
            self.fsm.current = self.state
            getattr(self.fsm, action_name)()
            self.state = self.fsm.current
            print "change state to: " + self.state

            getattr(self, action_name)(*args, **kwargs)
        else:
            raise RuntimeError("Action %s isn't available" % action_name)

    def is_runnable(self, ctx):
        return False

    def is_configurable(self, ctx):
        return True

    def is_configurated(self, ctx):
        return False

    def is_visible(self, ctx):
        return True


class FetchGseForm(forms.Form):
    # Add custom validator to check GSE prefix
    #  If file is fetched or is being fetched don't allow to change geo_uid
    geo_uid = forms.CharField(min_length=4, max_length=31, required=True)
    expression_set_name = forms.CharField(label="Name for expression set",
                                          max_length=255)
    gpl_annotation_name = forms.CharField(label="Name for GPL annotation",
                                          max_length=255)

    def clean_geo_uid(self):
        data = self.cleaned_data['geo_uid']
        if data[:3].upper() != 'GSE' not in data:
            raise forms.ValidationError("Geo uid should have 'GSE' prefix")

        # Always return the cleaned data, whether you have changed it or
        # not.
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
    widget = "widgets/fetch_ncbi_gse.html"
    pages = {
        "assign_sample_classes": "widgets/fetch_gse/assign_sample_classes.html",
    }
    form_data = {
        "expression_set_name": "expression",
        "gpl_annotation_name": "annotation",
    }

    def __init__(self):
        super(FetchGSE, self).__init__("Fetch ncbi gse", "user_input")

        self.form_cls = FetchGseForm
        self.form = self.form_cls(self.form_data)

        self.source_file = None

        self.celery_task_fetch = None
        self.celery_task_preprocess = None

        self.errors = []
        self.state = 'created'

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

    def get_provided_objects(self):
        return {
            "ExpressionSet": self.form["expression_set_name"].value(),
            "PlatformAnnotation": self.form["gpl_annotation_name"].value(),
        }

    def is_form_fields_editable(self):
        if self.state in ['created', 'form_modified']:
            return True
        return False

    def is_sub_pages_visible(self):
        if self.state in ['source_was_preprocessed', 'sample_classes_assigned']:
            return True
        return False

    def clean_errors(self):
        self.errors = []

    def validate(self):
        if self.form.is_valid():
            #TODO: additional checks e.g. other blocks doesn't provide
            #      variables with the same names
            self.do_action("on_form_is_valid")
            return
        self.do_action("on_form_not_valid")

    def on_form_is_valid(self):
        self.errors = []

    def on_form_not_valid(self):
        pass

    def save_form(self, exp, ctx, request, *args, **kwargs):
        print request.POST
        print self.state
        self.form = self.form_cls(request.POST)
        self.validate()
        print self.state
        exp.store_block(self)

    def show_form(self, exp, ctx, *args, **kwargs):
        exp.store_block(self)

    def reset_form(self, exp, *args, **kwargs):
        self.clean_errors()
        self.form = self.form_cls(self.form_data)
        self.is_form_visible = True
        self.file_can_be_fetched = False
        exp.store_block(self)

    def start_fetch(self, exp, ctx, *args, **kwargs):
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

    def assign_sample_classes(self, exp, ctx, request):
        pheno_df = self.expression_set.get_pheno_data_frame()
        sample_classes = json.loads(request.POST['sample_classes'])
        pheno_df['User_class'] = pd.Series(sample_classes)

        self.expression_set.store_pheno_data_frame(pheno_df)
        exp.store_block(self)

    def revoke_task(self, exp, *args, **kwargs):
        pass
        #self.celery_task_fetch.revoke(terminate=True)
        #exp.store_block(self)


class AssignSampleClasses(GenericBlock):
    fsm = Fysom({
        'events': [
            {'name': 'bind_variable', 'src': 'created', 'dst': 'variable_bound'},
            {'name': 'assign_samples', 'src': 'variable_bound', 'dst': 'some_samples_assigned'},
            {'name': 'complete_assignement', 'src': 'some_samples_assigned', 'dst': 'assignement_done'},
        ]
    })


    def __init__(self, *args, **kwargs):
        super(AssignSampleClasses, self).__init__(*args, **kwargs)
        self.state = 'created'

block_classes_by_name = {
    "fetch_ncbi_gse": FetchGSE,
    "assign_sample_classes": AssignSampleClasses,
}


def get_block_class_by_name(name):
    if name in block_classes_by_name.keys():
        return block_classes_by_name[name]
    else:
        raise KeyError("No such plugin: %s" % name)
