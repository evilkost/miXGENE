import json

import pandas as pd
from webapp.notification import BlockUpdated
from webapp.tasks import wrapper_task
from workflow.common_tasks import fetch_geo_gse, preprocess_soft

from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, execute_block_actions_list, OutputBlockField


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

        ActionRecord("start_preprocess", ["source_was_fetched"], "source_is_being_fetched", "Run preprocess"),
        ActionRecord("error_during_preprocess", ["source_is_being_fetched"], "source_was_fetched",
                     reload_block_in_client=True),
        ActionRecord("successful_preprocess", ["source_is_being_fetched"], "source_was_preprocessed",
                     reload_block_in_client=True),

        ActionRecord("assign_sample_classes", ["source_was_preprocessed", "done"], "done"),
    ])

    source_file = BlockField("source_file", FieldType.CUSTOM, None)
    # TODO: add sub page field
    pages = BlockField("pages", FieldType.RAW, init_val={
        "assign_sample_classes": {
            "title": "Assign sample classes",
            "resource": "assign_sample_classes",
            "widget": "widgets/fetch_gse/assign_sample_classes.html"
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
        # print "OUT_DATA: ", self._out_data
        self.clean_errors()
        exp.store_block(self)

        msg = BlockUpdated(self.exp_id, self.uuid, self.base_name)
        msg.comment = u"Dataset %s was preprocessed, \n please assign samples to classes" % self.geo_uid
        msg.silent = False
        msg.send()

    def assign_sample_classes(self, exp, request, *args, **kwargs):
        #TODO: Shift to celery
        es = self.get_out_var("expression_set")
        pheno_df = es.get_pheno_data_frame()
        sample_classes = json.loads(request.POST['sample_classes'])
        pheno_df[es.pheno_metadata["user_class_title"]] = pd.Series(sample_classes)

        es.store_pheno_data_frame(pheno_df)
        exp.store_block(self)
