import json
import logging

import pandas as pd
from environment.structures import prepare_phenotype_for_js_from_es
from webapp.notification import BlockUpdated
from webapp.tasks import wrapper_task
from workflow.blocks.fields import FieldType, BlockField, OutputBlockField, InputType, ParamField, ActionRecord, \
    ActionsList
from workflow.common_tasks import fetch_geo_gse, preprocess_soft

from workflow.blocks.generic import GenericBlock, save_params_actions_list, execute_block_actions_list

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

class FetchGSE(GenericBlock):
    block_base_name = "FETCH_GEO"
    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "valid_params"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),

        ActionRecord("start_fetch", ["valid_params", "done"], "source_is_being_fetched", "Start fetch"),
        ActionRecord("error_during_fetch", ["source_is_being_fetched"], "form_valid", reload_block_in_client=True),
        ActionRecord("successful_fetch", ["source_is_being_fetched"], "source_was_fetched", reload_block_in_client=True),

        ActionRecord("start_preprocess", ["source_was_fetched", "source_was_preprocessed"], "source_is_being_fetched", "Run preprocess"),
        ActionRecord("error_during_preprocess", ["source_is_being_fetched"], "source_was_fetched",
                     reload_block_in_client=True),
        ActionRecord("successful_preprocess", ["source_is_being_fetched"], "source_was_preprocessed",
                     reload_block_in_client=True),

        ActionRecord("assign_sample_classes", ["source_was_preprocessed", "done"], "done"),
    ])

    source_file = BlockField("source_file", FieldType.CUSTOM, None)

    pages = BlockField("pages", FieldType.RAW, init_val={
        "assign_phenotype_classes": {
            "title": "Assign phenotype classes",
            "resource": "assign_phenotype_classes",
            "widget": "widgets/assign_phenotype_classes.html"
        },
    })
    _is_sub_pages_visible = BlockField("is_sub_pages_visible", FieldType.RAW,
                                       init_val=False, is_a_property=True)

    ### PARAMETERS
    geo_uid = ParamField("geo_uid", "Geo accession id",
                         InputType.TEXT, FieldType.STR, "")

    _expression_set = OutputBlockField(name="expression_set", field_type=FieldType.HIDDEN,
                                provided_data_type="ExpressionSet")
    _gpl_annotation = OutputBlockField(name="gpl_annotation", field_type=FieldType.HIDDEN,
                                provided_data_type="PlatformAnnotation")

    def __init__(self, *args, **kwargs):
        super(FetchGSE, self).__init__("Fetch ncbi gse", *args, **kwargs)
        self.celery_task_fetch = None
        self.celery_task_preprocess = None

    def is_form_fields_editable(self):
        if self.state in ['created', 'form_modified']:
            return True
        return False

    def phenotype_for_js(self, exp, *args, **kwargs):
        headers_options = {
            "custom_title_prefix_map": [
                ("Sample_title", "Title"),
                ("Sample_description", "Description"),
                ("Sample_characteristics", "Characteristics"),
                ("Sample_organism", "Organism"),
                ("Sample_geo_accession", "GEO #"),

                ("Sample_", ""),
            ],
            "prefix_order": [
                "Sample_geo_accession",
                "Sample_title",
                "Sample_description",
                "Sample_contact",
                "Sample_characteristics",
            ],
            "prefix_hide": {
                "Sample_contact",
                "Sample_channel",
                "Sample_data_row_count",
                "Sample_data",
                "Sample_platform",
                "Sample_growth",
                "Sample_series_id",
                "Sample_status",
                "Sample_extract",
                "Sample_supplementary_file",
                "Sample_hyb",
                "Sample_label",
                "Sample_source",
                "Sample_last_update",
                "Sample_molecule",
                "Sample_organism",
                "Sample_scan",
                "Sample_taxid",
                "Sample_type",
                "Sample_submission",
            }
        }
        return prepare_phenotype_for_js_from_es(
            self.get_out_var("expression_set"), headers_options)

    @property
    def is_sub_pages_visible(self):
        if self.state in ['source_was_preprocessed', 'sample_classes_assigned', 'ready']:
            return True
        return False

    def start_fetch(self, exp, *args, **kwargs):
        """
            @param exp: Experiment
        """
        self.clean_errors()
        self.celery_task_fetch = wrapper_task.s(
            fetch_geo_gse, exp, self,
            geo_uid=self.geo_uid,
            success_action="successful_fetch", error_action="error_during_fetch",
            ignore_cache=False
        )
        exp.store_block(self)
        self.celery_task_fetch.apply_async()

    def error_during_fetch(self, exp, *args, **kwargs):
        exp.store_block(self)

    def successful_fetch(self, exp, source_file, *args, **kwargs):
        self.clean_errors()
        self.source_file = source_file
        self.do_action("start_preprocess", exp)
        exp.store_block(self)

    def start_preprocess(self, exp, *args, **kwargs):
        self.celery_task_preprocess = wrapper_task.s(
            preprocess_soft,
            exp, self,
            source_file=self.source_file,
            success_action="successful_preprocess",
            error_action="error_during_preprocess"
        )
        exp.store_block(self)
        self.celery_task_preprocess.apply_async()

    def error_during_preprocess(self, exp, *args, **kwargs):
        exp.store_block(self)

    def successful_preprocess(self, exp, es, ann, *args, **kwargs):
        """
            @type es: ExpressionSet
            @type ann: PlatformAnnotation
        """
        self.set_out_var("expression_set", es)
        self.set_out_var("gpl_annotation", ann)

        self.clean_errors()
        exp.store_block(self)

        msg = BlockUpdated(self.exp_id, self.uuid, self.base_name)
        msg.comment = u"Dataset %s was preprocessed, \n please assign samples to classes" % self.geo_uid
        msg.silent = False
        msg.send()

    def update_user_classes_assignment(self, exp, request, *args, **kwargs):
        #TODO: unify code with user upload
        es = self.get_out_var("expression_set")
        pheno_df = es.get_pheno_data_frame()

        received = json.loads(request.body)
        es.pheno_metadata["user_class_title"] = received["user_class_title"]
        pheno_df[received["user_class_title"]] = received["classes"]

        es.store_pheno_data_frame(pheno_df)
        exp.store_block(self)

        self.do_action("assign_sample_classes", exp)

    def assign_sample_classes(self, exp, *args, **kwargs):
        pass
