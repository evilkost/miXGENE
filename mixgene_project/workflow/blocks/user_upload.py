# coding: utf-8
import json
import logging
import traceback
import sys

import pandas as pd

from environment.structures import ExpressionSet, BinaryInteraction, prepare_phenotype_for_js_from_es
from environment.structures import GmtStorage, GeneSets

from workflow.blocks.fields import FieldType, BlockField, OutputBlockField, InputType, ParamField, ActionRecord, \
    ActionsList
from workflow.blocks.generic import GenericBlock

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class UserUpload(GenericBlock):
    block_base_name = "UPLOAD"
    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "valid_params"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),

        ActionRecord("process_upload", ["valid_params", "processing_upload"],
                     "processing_upload", "Process uploaded data", reload_block_in_client=True),
        ActionRecord("success", ["processing_upload"], "done", reload_block_in_client=True),
        ActionRecord("error", ["processing_upload"], "valid_params", reload_block_in_client=True),
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
    _is_sub_pages_visible = BlockField("is_sub_pages_visible", FieldType.RAW, is_a_property=True)

    ### PARAMETERS
    _expression_set = OutputBlockField(name="expression_set", field_type=FieldType.HIDDEN,
                                       provided_data_type="ExpressionSet")
    _gpl_annotation = OutputBlockField(name="gpl_annotation", field_type=FieldType.HIDDEN,
                                       provided_data_type="PlatformAnnotation")

    # TODO: COPY PASTE from fetch_gse block
    pages = BlockField("pages", FieldType.RAW, init_val={
        "assign_phenotype_classes": {
            "title": "Assign phenotype classes",
            "resource": "assign_phenotype_classes",
            "widget": "widgets/assign_phenotype_classes.html"
        },
    })

    def __init__(self, *args, **kwargs):
        super(UserUpload, self).__init__("User upload", *args, **kwargs)


    @property
    def is_sub_pages_visible(self):
        if self.state in ['source_was_preprocessed', 'sample_classes_assigned', 'ready', 'done']:
            return True
        return False

    def phenotype_for_js(self, exp, *args, **kwargs):
        return prepare_phenotype_for_js_from_es(self.get_out_var("expression_set"))

    def update_user_classes_assignment(self, exp, request, *args, **kwargs):
        es = self.get_out_var("expression_set")
        pheno_df = es.get_pheno_data_frame()

        received = json.loads(request.body)
        es.pheno_metadata["user_class_title"] = received["user_class_title"]
        pheno_df[received["user_class_title"]] = received["classes"]

        es.store_pheno_data_frame(pheno_df)
        exp.store_block(self)

    def process_upload(self, exp, *args, **kwargs):
        """
            @param exp: Experiment
        """
        self.clean_errors()

        assay_df = pd.DataFrame.from_csv(self.es_matrix.get_file())

        es = ExpressionSet(base_dir=exp.get_data_folder(),
                           base_filename="%s_annotation" % self.uuid)

        pheno_df = pd.DataFrame.from_csv(self.pheno_matrix.get_file())
        pheno_df.set_index(pheno_df.columns[0])

        user_class_title = es.pheno_metadata["user_class_title"]
        if user_class_title not in pheno_df.columns:
            pheno_df[es.pheno_metadata["user_class_title"]] = ""

        es.store_assay_data_frame(assay_df)
        es.store_pheno_data_frame(pheno_df)

        if self.working_unit:
            es.working_unit = self.working_unit

        self.set_out_var("expression_set", es)

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
        ActionRecord("success", ["processing_upload"], "done", reload_block_in_client=True),
        ActionRecord("error", ["processing_upload"], "valid_params"),
    ])

    m_rna_matrix = ParamField("m_rna_matrix", title="mRNA expression", order_num=10,
                         input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM)
    m_rna_platform = ParamField("m_rna_platform", title="Platform ID", order_num=11,
                               input_type=InputType.TEXT, field_type=FieldType.STR, required=False)
    m_rna_unit = ParamField("m_rna_unit", title="Working unit [used when platform is unknown]", init_val=None,
                           order_num=12, input_type=InputType.TEXT, field_type=FieldType.STR, required=False)

    mi_rna_matrix = ParamField("mi_rna_matrix", title=u"μRNA expression", order_num=20,
                          input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM, required=False)

    methyl_matrix = ParamField("methyl_matrix", title="Methylation expression", order_num=30,
                          input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM, required=False)

    pheno_matrix = ParamField("pheno_matrix", title="Phenotype matrix", order_num=40,
                              input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM, required=False)

    csv_sep = ParamField(
        "csv_sep", title="CSV separator symbol", order_num=50,
        input_type=InputType.SELECT, field_type=FieldType.STR, init_val=",",
        options={
            "inline_select_provider": True,
            "select_options": [
                [" ", "space ( )"],
                [",", "comma  (,)"],
                ["\t", "tab (\\t)"],
                [";", "semicolon (;)"],
                [":", "colon (:)"],
            ]
        }
    )

    _is_sub_pages_visible = BlockField("is_sub_pages_visible", FieldType.RAW, is_a_property=True)

    _m_rna_es = OutputBlockField(name="m_rna_es", field_type=FieldType.HIDDEN,
        provided_data_type="ExpressionSet")
    _m_rna_annotation = OutputBlockField(name="m_rna_annotation", field_type=FieldType.HIDDEN,
        provided_data_type="PlatformAnnotation")
    _mi_rna_es = OutputBlockField(name="mi_rna_es", field_type=FieldType.HIDDEN,
                                provided_data_type="ExpressionSet")
    _methyl_es = OutputBlockField(name="methyl_es", field_type=FieldType.HIDDEN,
                                 provided_data_type="ExpressionSet")

    pages = BlockField("pages", FieldType.RAW, init_val={
        "assign_phenotype_classes": {
            "title": "Assign phenotype classes",
            "resource": "assign_phenotype_classes",
            "widget": "widgets/assign_phenotype_classes.html"
        },
    })

    def __init__(self, *args, **kwargs):
        super(UserUploadComplex, self).__init__("User upload", *args, **kwargs)

    @property
    def is_sub_pages_visible(self):
        if self.state in ['source_was_preprocessed', 'sample_classes_assigned', 'ready', 'done']:
            return True
        return False

    def process_upload(self, exp, *args, **kwargs):
        """
            @param exp: Experiment
        """
        # TODO: move to celery
        self.clean_errors()
        sep = getattr(self, "csv_sep", " ")

        try:
            if not self.pheno_matrix:
                self.warnings.append(Exception("Phenotype is undefined"))
                pheno_df = None
            else:
                pheno_df = self.pheno_matrix.get_as_data_frame(sep)
                pheno_df.set_index(pheno_df.columns[0])

                # TODO: solve somehow better: Here we add empty column with user class assignment
                pheno_df[ExpressionSet(None, None).pheno_metadata["user_class_title"]] = ""

            if self.m_rna_matrix is not None:
                m_rna_assay_df = self.m_rna_matrix.get_as_data_frame(sep)

                m_rna_es = ExpressionSet(base_dir=exp.get_data_folder(),
                                        base_filename="%s_m_rna_es" % self.uuid)
                m_rna_es.store_assay_data_frame(m_rna_assay_df)
                m_rna_es.store_pheno_data_frame(pheno_df)
                m_rna_es.working_unit = self.m_rna_unit

                self.set_out_var("m_rna_es", m_rna_es)

                # TODO: fetch GPL annotation if GPL id was provided

            if self.mi_rna_matrix is not None:
                mi_rna_assay_df = self.mi_rna_matrix.get_as_data_frame(sep)

                mi_rna_es = ExpressionSet(base_dir=exp.get_data_folder(),
                                        base_filename="%s_mi_rna_es" % self.uuid)
                mi_rna_es.store_assay_data_frame(mi_rna_assay_df)
                mi_rna_es.store_pheno_data_frame(pheno_df)

                self.set_out_var("mi_rna_es", mi_rna_es)

            if self.methyl_matrix is not None:

                methyl_assay_df = self.methyl_matrix.get_as_data_frame(sep)

                methyl_es = ExpressionSet(base_dir=exp.get_data_folder(),
                                          base_filename="%s_methyl_es" % self.uuid)
                methyl_es.store_assay_data_frame(methyl_assay_df)
                methyl_es.store_pheno_data_frame(pheno_df)

                self.set_out_var("methyl_es", methyl_es)

            self.do_action("success", exp)
        except Exception as e:
            ex_type, ex, tb = sys.exc_info()
            traceback.print_tb(tb)
            self.do_action("error", exp, e)
        # self.celery_task_fetch.apply_async()

    def phenotype_for_js(self, exp, *args, **kwargs):
        m_rna_es = self.get_out_var("m_rna_es")
        mi_rna_es = self.get_out_var("mi_rna_es")
        methyl_es = self.get_out_var("methyl_es")
        es = None
        if m_rna_es is not None:
            es = m_rna_es
        elif mi_rna_es is not None:
            es = mi_rna_es
        elif methyl_es is not None:
            es = methyl_es

        if es is None:
            raise Exception("No data was stored before")

        return prepare_phenotype_for_js_from_es(es)

    def update_user_classes_assignment(self, exp, request, *args, **kwargs):
        m_rna_es = self.get_out_var("m_rna_es")
        mi_rna_es = self.get_out_var("mi_rna_es")
        methyl_es = self.get_out_var("methyl_es")
        es = None
        if m_rna_es is not None:
            es = m_rna_es
        elif mi_rna_es is not None:
            es = mi_rna_es
        elif methyl_es is not None:
            es = methyl_es

        if es is None:
            raise Exception("No data was stored before")

        pheno_df = es.get_pheno_data_frame()

        received = json.loads(request.body)

        pheno_df[received["user_class_title"]] = received["classes"]

        for work_es in [m_rna_es, mi_rna_es, methyl_es]:
            if work_es is not None:
                work_es.pheno_metadata["user_class_title"] = received["user_class_title"]
                work_es.store_pheno_data_frame(pheno_df)

        # import ipdb; ipdb.set_trace()
        exp.store_block(self)

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


class UploadGeneSets(GenericBlock):
    block_base_name = "GENE_SETS_UPLOAD"
    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "done"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),
        ])

    upload_gs = ParamField(
        "upload_gs", title="Gene sets in .gmt format", order_num=10,
        input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM
    )

    set_units = ParamField("set_units", title="Set units",
                           order_num=11, input_type=InputType.TEXT, field_type=FieldType.STR, required=False)
    gen_units = ParamField("gen_units", title="Gene units",
                           order_num=12, input_type=InputType.TEXT, field_type=FieldType.STR, required=False)

    _gene_sets = OutputBlockField(name="gene_sets", provided_data_type="GeneSets")

    def __init__(self, *args, **kwargs):
        super(UploadGeneSets, self).__init__("Upload gene sets", *args, **kwargs)

    def on_params_is_valid(self, exp, *args, **kwargs):
        try:
            gmt_file = self.upload_gs.get_file()
            gs = GmtStorage.read_inp(gmt_file, "\t")
            gene_sets = GeneSets(exp.get_data_folder(), str(self.uuid))
            gene_sets.store_gs(gs)

            self.set_out_var("gene_sets", gene_sets)

        except Exception as e:
            log.error(e)

        exp.store_block(self)
