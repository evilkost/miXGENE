# coding: utf-8
import traceback

import pandas as pd
import sys

from environment.structures import ExpressionSet, BinaryInteraction
from workflow.blocks.generic import GenericBlock, ActionsList, save_params_actions_list, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, execute_block_actions_list, OutputBlockField


class UserUpload(GenericBlock):
    block_base_name = "UPLOAD"
    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "valid_params"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),

        ActionRecord("process_upload", ["valid_params", "processing_upload"],
                     "processing_upload", "Process uploaded data"),
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


class UserUploadComplex(GenericBlock):
    block_base_name = "UPLOAD_CMPLX"
    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "valid_params"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),

        ActionRecord("process_upload", ["valid_params", "processing_upload"],
                     "processing_upload", "Process uploaded data"),
        ActionRecord("success", ["processing_upload"], "done"),
        ActionRecord("error", ["processing_upload"], "valid_params"),
    ])

    mRNA_matrix = ParamField("mRNA_matrix", title="mRNA expression", order_num=10,
                         input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM)
    mRNA_platform = ParamField("mRNA_platform", title="Platform ID", order_num=11,
                               input_type=InputType.TEXT, field_type=FieldType.STR, required=False)
    mRNA_unit = ParamField("mRNA_unit", title="Working unit [used when platform is unknown]", init_val=None,
                           order_num=12, input_type=InputType.TEXT, field_type=FieldType.STR, required=False)

    miRNA_matrix = ParamField("miRNA_matrix", title=u"μRNA expression", order_num=20,
                          input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM, required=False)

    methyl_matrix = ParamField("methyl_matrix", title="Methylation expression", order_num=30,
                          input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM, required=False)

    pheno_matrix = ParamField("pheno_matrix", title="Phenotype matrix", order_num=40,
                              input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM, required=False)

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


    _mRNA_es = OutputBlockField(name="mRNA_es", field_type=FieldType.HIDDEN,
        provided_data_type="ExpressionSet")

    _mRNA_annotation = OutputBlockField(name="mRNA_annotation", field_type=FieldType.HIDDEN,
        provided_data_type="PlatformAnnotation")

    _miRNA_es = OutputBlockField(name="miRNA_es", field_type=FieldType.HIDDEN,
                                provided_data_type="ExpressionSet")

    _methyl_es = OutputBlockField(name="methyl_es", field_type=FieldType.HIDDEN,
                                 provided_data_type="ExpressionSet")


    # TODO: COPY PASTE from fetch_gse block
    # pages = BlockField("pages", FieldType.RAW, init_val={
    #     "assign_sample_classes": {
    #         "title": "Assign sample classes",
    #         "resource": "assign_sample_classes",
    #         "widget": "widgets/fetch_gse/assign_sample_classes.html"
    #     },
    # })

    def __init__(self, *args, **kwargs):
        super(UserUploadComplex, self).__init__("User upload", *args, **kwargs)

    @property
    def is_sub_pages_visible(self):
        if self.state in ['source_was_preprocessed', 'sample_classes_assigned', 'ready']:
            return True
        return False

    def process_upload(self, exp, *args, **kwargs):
        """
            @param exp: Experiment
        """
        # TODO: move to celery
        self.clean_errors()
        try:
            if not self.pheno_matrix:
                self.warnings.append(Exception("Phenotype is undefined"))
                pheno_df = None
            else:
                pheno_df = self.pheno_matrix.get_as_data_frame()
                pheno_df.set_index(pheno_df.columns[0])

            if self.mRNA_matrix is not None:
                mRNA_assay_df = self.mRNA_matrix.get_as_data_frame()

                mRNA_es = ExpressionSet(base_dir=exp.get_data_folder(),
                                        base_filename="%s_mRNA_es" % self.uuid)
                mRNA_es.store_assay_data_frame(mRNA_assay_df)
                mRNA_es.store_pheno_data_frame(pheno_df)
                mRNA_es.working_unit = self.mRNA_unit

                self.set_out_var("mRNA_es", mRNA_es)

                # TODO: fetch GPL annotation if GPL id was provided

            if self.miRNA_matrix is not None:
                miRNA_assay_df = self.miRNA_matrix.get_as_data_frame()

                miRNA_es = ExpressionSet(base_dir=exp.get_data_folder(),
                                        base_filename="%s_miRNA_es" % self.uuid)
                miRNA_es.store_assay_data_frame(miRNA_assay_df)
                miRNA_es.store_pheno_data_frame(pheno_df)

                self.set_out_var("miRNA_es", miRNA_es)

            if self.methyl_matrix is not None:
                methyl_assay_df = self.methyl_matrix.get_as_data_frame()

                methyl_es = ExpressionSet(base_dir=exp.get_data_folder(),
                                          base_filename="%s_methyl_es" % self.uuid)
                miRNA_es.store_assay_data_frame(methyl_assay_df)
                miRNA_es.store_pheno_data_frame(pheno_df)

                self.set_out_var("methyl_es", methyl_es)

            self.do_action("success", exp)
        except Exception as e:
            ex_type, ex, tb = sys.exc_info()
            traceback.print_tb(tb)
            self.do_action("error", exp, e)
        # self.celery_task_fetch.apply_async()

    def success(self, exp, *args, **kwargs):
        pass


class UploadInteraction(GenericBlock):
    block_base_name = "GENE_INTERACTION"
    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "done"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),
    ])

    upload_interaction = ParamField("upload_interaction", title="Interaction matrix", order_num=10,
        input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM)
    row_units = ParamField("row_units", title="Row units",
        order_num=11, input_type=InputType.TEXT, field_type=FieldType.STR, required=False)
    col_units = ParamField("col_units", title="Column units",
                           order_num=12, input_type=InputType.TEXT, field_type=FieldType.STR, required=False)

    _interaction = OutputBlockField(name="interaction", provided_data_type="BinaryInteraction")

    def __init__(self, *args, **kwargs):
        super(UploadInteraction, self).__init__("Upload gene interaction matrix", *args, **kwargs)

    def on_params_is_valid(self, exp, *args, **kwargs):
        # Convert to  BinaryInteraction
        interaction_df = self.upload_interaction.get_as_data_frame()

        interaction = BinaryInteraction(exp.get_data_folder(), str(self.uuid))
        interaction.store_matrix(interaction_df)

        interaction.row_units = self.row_units
        interaction.col_units = self.col_units

        self.set_out_var("interaction", interaction)
        exp.store_block(self)