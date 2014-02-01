from collections import defaultdict
import json

from django import forms
from fysom import Fysom
import pandas as pd

from webapp.models import Experiment
from environment.structures import SequenceContainer

from workflow.common_tasks import generate_cv_folds
from workflow.ports import BlockPort

from generic import GenericBlock



class CrossValidationForm(forms.Form):
    folds_num = forms.IntegerField(min_value=2, max_value=100)
    #split_ratio = forms.FloatField(min_value=0, max_value=1)

class CrossValidation(GenericBlock):
    fsm = Fysom({
        'events': [
            {'name': 'bind_variables', 'src': 'created', 'dst': 'variable_bound'},
            {'name': 'bind_variables', 'src': 'finished', 'dst': 'variable_bound'},
            {'name': 'bind_variables', 'src': 'variable_bound', 'dst': 'variable_bound'},

            {'name': 'bind_variables', 'src': 'form_modified', 'dst': 'variable_bound'},

            {'name': 'save_form', 'src': 'variable_bound', 'dst': 'form_modified'},
            {'name': 'save_form', 'src': 'form_modified', 'dst': 'form_modified'},

            {'name': 'on_form_is_valid', 'src': 'form_modified', 'dst': 'form_valid'},
            {'name': 'on_form_not_valid', 'src': 'form_modified', 'dst': 'form_modified'},

            {'name': 'reset_form', 'src': 'form_modified', 'dst': 'variable_bound'},
            {'name': 'reset_form', 'src': 'form_valid', 'dst': 'variable_bound'},

            {'name': 'show_form', 'src': 'form_valid', 'dst': 'form_modified'},


            {'name': 'generate_folds', 'src': 'form_valid', 'dst': 'generating_folds'},
            {'name': 'generate_folds', 'src': 'generated_folds', 'dst': 'generating_folds'},
            {'name': 'on_generate_folds_done', 'src': 'generating_folds', 'dst': 'generated_folds'},
            {'name': 'on_generate_folds_error', 'src': 'generating_folds', 'dst': 'form_valid'},


            {'name': 'push_next_fold', 'src': 'generated_folds', 'dst': 'processing_fold'},
            {'name': 'push_next_fold', 'src': 'fold_result_collected', 'dst': 'processing_fold'},

            {'name': 'collect_fold_result', 'src': 'processing_fold', 'dst': 'fold_result_collected'},

            {'name': 'all_folds_processed', 'src': 'fold_result_collected', 'dst': 'cv_done'},


            {'name': 'run_sub_blocks', 'src': 'split_dataset', 'dst': 'split_dataset'},
            {'name': 'run_sub_blocks', 'src': 'split_dataset', 'dst': 'finished'},

        ]
    })
    widget = "widgets/cross_validation_base.html"
    elements = [
        "cv_info.html"
    ]
    form_cls = CrossValidationForm
    block_base_name = "CROSS_VALID"
    all_actions = [
        ("bind_variables", "Select input ports", True),
        ("bind_inner_variables", "Select inner ports", True),
        ("save_form", "Save parameters", True),

        ("on_form_is_valid", "", False),
        ("on_form_not_valid", "", False),

        ("reset_form", "Reset parameters", True),


        ("generate_folds", "Generate folds", True),
        ("on_generate_folds_done", "", False),
        ("on_generate_folds_error", "", False),

        ("push_next_fold", "Process next fold", True),
        ("collect_fold_result", "Collect fold result", True),
        ("all_folds_processed", "", False),


        ("success", "", False),
        ("error", "", False)

    ]
    create_new_scope = True

    provided_objects = {}
    provided_objects_inner = {
        "es_train_i": "ExpressionSet",
        "es_test_i": "ExpressionSet",
        }

    params_prototype = {
        "folds_num": {
            "name": "folds_num",
            "title": "Folds number",
            "input_type": "text",
            "validation": None,
            "default": 10,
            },
        # "split_ratio": {
        #     "name": "split_ratio",
        #     "title": "Train/Test ratio",
        #     "input_type": "slider",
        #     "min_value": 0,
        #     "max_value": 1,
        #     "step": 0.01,
        #     "default": 0.7,
        #     "validation": None
        # }
    }

    @property
    def sub_blocks(self):
        uuids_blocks = Experiment.get_blocks(self.children_blocks)
        exp = Experiment.objects.get(e_id=self.exp_id)
        result = []
        for uuid, block in uuids_blocks:
            block.before_render(exp)
            result.append((uuid, block))

        return result

    @property
    def sub_scope(self):
        return "%s_%s" % (self.scope, self.uuid)

    def __init__(self, *args, **kwargs):
        super(CrossValidation, self).__init__("Cross Validation", *args, **kwargs)
        self.bound_variable_field = None
        self.bound_variable_block = None
        self.bound_variable_block_alias = None

        self.children_blocks = []

        self.ports = {
            "input": {
                "es": BlockPort(name="es", title="Choose expression set",
                                data_type="ExpressionSet", scopes=[self.scope]),
                # "ann": BlockPort(name="ann", title="Choose annotation",
                #                  data_type="PlatformAnnotation", scopes=[self.scope])

            },
            "collect_internal": {
                "result": BlockPort(name="result", title="Choose classifier result",
                                    data_type="mixML", scopes=[self.scope, self.sub_scope]),

            }
        }

        #  TODO: fix by introducing register_var method to class / metaclass
        #   or at least method to look through params_prototype and popultions params with default values
        # self.params["split_ratio"] = self.params_prototype["split_ratio"]["default"]
        self.params["folds_num"] = self.params_prototype["folds_num"]["default"]

        self.sequence = SequenceContainer(fields=self.provided_objects_inner)
        self.results = []

    ### inner variables provider
    @property
    def es_train_i(self):
        return self.sequence.get_field("es_train_i")

    @property
    def es_test_i(self):
        return self.sequence.get_field("es_test_i")

    ### end inner variables


    #TODO: debug
    @property
    def current_fold_idx(self):
        return self.sequence.iterator

    def serialize(self, exp, to="dict"):
        hash = super(CrossValidation, self).serialize(exp, to)
        hash["current_fold_idx"] = self.current_fold_idx
        hash["results"] = self.results

        return hash

    # def before_render(self, exp, *args, **kwargs):
    #     context_add = super(CrossValidation, self).before_render(exp, *args, **kwargs)
    #     available = exp.get_visible_variables(scopes=[self.scope], data_types=["ExpressionSet"])
    #
    #     self.variable_options = prepare_bound_variable_select_input(
    #         available, exp.get_block_aliases_map(),
    #         self.bound_variable_block_alias, self.bound_variable_field)
    #
    #     if len(self.variable_options) == 0:
    #         self.errors.append(Exception("There is no blocks which provides Expression Set"))
    #
    #     return context_add

    def reset_form(self, exp, *args, **kwargs):
        self.clean_errors()
        exp.store_block(self)

    def generate_folds(self, exp, request, *args, **kwargs):
        self.clean_errors()

        es = self.get_var_by_bound_key_str(exp, self.ports["input"]["es"].bound_key)
        # ann = self.get_var_by_bound_key_str(exp, self.ports["input"]["ann"].bound_key)
        # TODO: keep actual BoundVar object

        self.celery_task = generate_cv_folds.s(exp, self,
                                               self.params["folds_num"],
                                               # self.params["split_ratio"],
                                               es)
        exp.store_block(self)
        self.celery_task.apply_async()

    def push_next_fold(self, exp, *args, **kwargs):
        self.sequence.apply_next()
        exp.store_block(self)

    def collect_fold_result(self, exp, *args, **kwargs):
        ml_res = self.get_var_by_bound_key_str(exp,
                                               self.ports["collect_internal"]["result"].bound_key
        )
        self.results.append(ml_res.acc)
        exp.store_block(self)
        if self.sequence.is_end():
            self.do_action("all_folds_processed", exp)
        else:
            self.do_action("push_next_fold", exp)

    def on_generate_folds_error(self, exp, *args, **kwargs):
        exp.store_block(self)

    def on_generate_folds_done(self, exp, *args, **kwargs):
        self.clean_errors()
        exp.store_block(self)

    def all_folds_processed(self, exp, *args, **kwargs):
        exp.store_block(self)
