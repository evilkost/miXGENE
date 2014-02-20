from environment.structures import ExpressionSet

__author__ = 'kost'

import json

import pandas as pd
from workflow.common_tasks import fetch_geo_gse, preprocess_soft

from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, execute_block_actions_list, OutputBlockField


class UserUpload(GenericBlock):
    block_base_name = "UPLOAD"
    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "valid_params"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),

        ActionRecord("process_upload", ["valid_params", "processing_upload"], "processing_upload", "Process uploaded data"),
        ActionRecord("success", ["processing_upload"], "done"),
        ActionRecord("error", ["processing_upload"], "valid_params"),
    ])

    es_matrix = ParamField("es_matrix", title="Expression set matrix", order_num=0,
        input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM)
    pheno_matrix = ParamField("pheno_matrix", title="Phenotype matrix", order_num=1,
        input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM)
    gpl_platform = ParamField("gpl_platform", title="Platform ID", order_num=2,
        input_type=InputType.TEXT, field_type=FieldType.STR, required=False)
    working_unit = ParamField("working_unit", title="Working unit [used when platform is unknown]",
        order_num=3, input_type=InputType.TEXT, field_type=FieldType.STR, required=False)
    # TODO: add sub page field
    # pages = BlockField("pages", FieldType.RAW, init_val={
    #     "assign_sample_classes": {
    #         "title": "Assign sample classes",
    #         "resource": "assign_sample_classes",
    #         "widget": "widgets/fetch_gse/assign_sample_classes.html"
    #     },
    # })
    _is_sub_pages_visible = BlockField("is_sub_pages_visible", FieldType.RAW,
                                       init_val=False, is_a_property=True)

    ### PARAMETERS
    _expression_set = OutputBlockField(name="expression_set", field_type=FieldType.HIDDEN,
                                       provided_data_type="ExpressionSet")
    _gpl_annotation = OutputBlockField(name="gpl_annotation", field_type=FieldType.HIDDEN,
                                       provided_data_type="PlatformAnnotation")

    # TODO: COPY PASTE from fetch_gse block
    pages = BlockField("pages", FieldType.RAW, init_val={
        "assign_sample_classes": {
            "title": "Assign sample classes",
            "resource": "assign_sample_classes",
            "widget": "widgets/fetch_gse/assign_sample_classes.html"
        },
    })

    def __init__(self, *args, **kwargs):
        super(UserUpload, self).__init__("User upload", *args, **kwargs)


    @property
    def is_sub_pages_visible(self):
        if self.state in ['source_was_preprocessed', 'sample_classes_assigned', 'ready']:
            return True
        return False

    def process_upload(self, exp, *args, **kwargs):
        """
            @param exp: Experiment
        """
        self.clean_errors()

        assay_df = pd.DataFrame.from_csv(self.es_matrix.get_file())

        pheno_df = pd.DataFrame.from_csv(self.pheno_matrix.get_file())
        pheno_df.set_index(pheno_df.columns[0])

        es = ExpressionSet(base_dir=exp.get_data_folder(),
                           base_filename="%s_annotation" % self.uuid)

        es.store_assay_data_frame(assay_df)
        es.store_pheno_data_frame(pheno_df)

        if self.working_unit:
            es.working_unit = self.working_unit

        setattr(self, "expression_set", es)

        exp.store_block(self)

        self.do_action("success", exp)
        # self.celery_task_fetch.apply_async()

    def success(self, exp, *args, **kwargs):
        pass