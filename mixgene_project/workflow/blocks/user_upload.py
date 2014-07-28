# coding: utf-8
import json
import logging
import traceback
import sys

import pandas as pd
import redis_lock

from environment.structures import ExpressionSet, BinaryInteraction, prepare_phenotype_for_js_from_es
from environment.structures import GmtStorage, GeneSets
from mixgene.redis_helper import ExpKeys
from mixgene.util import get_redis_instance
from webapp.models import UploadedFileWrapper, UploadedData
from workflow.blocks.blocks_pallet import GroupType

from workflow.blocks.fields import FieldType, BlockField, OutputBlockField, InputType, ParamField, ActionRecord, \
    ActionsList, MultiUploadField
from workflow.blocks.generic import GenericBlock

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class UploadMixin(object):
    is_abstract = True
    block_group = GroupType.INPUT_DATA

    def save_file_input(self, exp, field_name, file_obj, multiple=False, upload_meta=None):
        if upload_meta is None:
            upload_meta = {}

        if not hasattr(self, field_name):
            raise Exception("Block doesn't have field: %s" % field_name)

        orig_name = file_obj.name
        local_filename = "%s_%s_%s" % (self.uuid[:8], field_name, file_obj.name)

        if not multiple:
            log.debug("Storing single upload to field: %s", field_name)
            ud, is_created = UploadedData.objects.get_or_create(
                exp=exp, block_uuid=self.uuid, var_name=field_name)

            file_obj.name = local_filename
            ud.data = file_obj
            ud.save()

            ufw = UploadedFileWrapper(ud.pk)
            ufw.orig_name = orig_name
            setattr(self, field_name, ufw)
            exp.store_block(self)
        else:
            log.debug("Adding upload to field: %s", field_name)

            ud, is_created = UploadedData.objects.get_or_create(
                exp=exp, block_uuid=self.uuid, var_name=field_name, filename=orig_name)

            file_obj.name = local_filename
            ud.data = file_obj
            ud.filename = orig_name
            ud.save()

            ufw = UploadedFileWrapper(ud.pk)
            ufw.orig_name = orig_name

            r = get_redis_instance()
            with redis_lock.Lock(r, ExpKeys.get_block_global_lock_key(self.exp_id, self.uuid)):
                log.debug("Enter lock, file: %s", orig_name)
                block = exp.get_block(self.uuid)
                attr = getattr(block, field_name)

                attr[orig_name] = ufw
                log.debug("Added upload `%s` to collection: %s", orig_name, attr.keys())
                exp.store_block(block)
                log.debug("Exit lock, file: %s", orig_name)

    def erase_file_input(self, exp, data):
        field_name = json.loads(data)["field_name"]
        field = self._block_spec.params.get(field_name)

        if not field.options.get("multiple", False):
            #  single stored value
            ufw = getattr(self, field_name)
            ud = ufw.ud
            ud.delete()
            setattr(self, field_name, None)
        else:
            # multiple
            ufw_dict = getattr(self, field_name)
            for name, ufw in ufw_dict.items():
                ufw.ud.delete()
            setattr(self, field_name, MultiUploadField())

        exp.store_block(self)


class GenericUploadBlock(GenericBlock, UploadMixin):
    is_abstract = True
    block_group = GroupType.INPUT_DATA


common_save_actions = ActionsList([
    ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                 user_title="Save parameters"),
    ActionRecord("on_params_is_valid", ["validating_params"], "done"),
    ActionRecord("on_params_not_valid", ["validating_params"], "created"),
])


class UserUploadComplex(GenericUploadBlock):
    block_base_name = "UPLOAD_CMPLX"
    name = "Upload mRna/miRna/methyl dataset"

    _block_actions = ActionsList([
        ActionRecord("save_params", ["created", "valid_params", "done", "ready"], "validating_params",
                     user_title="Save parameters"),
        ActionRecord("on_params_is_valid", ["validating_params"], "valid_params"),
        ActionRecord("on_params_not_valid", ["validating_params"], "created"),

        ActionRecord("process_upload", ["valid_params", "processing_upload"],
                     "processing_upload", "Process uploaded data"),
        ActionRecord("success", ["processing_upload"], "done", reload_block_in_client=True),
        ActionRecord("error", ["processing_upload"], "valid_params"),


        ActionRecord("reset", ["*"], "created", user_title="Reset"),
    ])

    m_rna_matrix = ParamField(name="m_rna_matrix", title="mRNA expression", order_num=10,
                         input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM)
    m_rna_platform = ParamField(name="m_rna_platform", title="Platform ID", order_num=11,
                               input_type=InputType.TEXT, field_type=FieldType.STR, required=False)
    m_rna_unit = ParamField(name="m_rna_unit", title="Working unit [used when platform is unknown]", init_val=None,
                           order_num=12, input_type=InputType.TEXT, field_type=FieldType.STR, required=False)

    mi_rna_matrix = ParamField(name="mi_rna_matrix", title=u"μRNA expression", order_num=20,
                          input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM, required=False)

    methyl_matrix = ParamField(name="methyl_matrix", title="Methylation expression", order_num=30,
                          input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM, required=False)

    pheno_matrix = ParamField(name="pheno_matrix", title="Phenotype matrix", order_num=40,
                              input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM, required=False)

    csv_sep = ParamField(
        name="csv_sep", title="CSV separator symbol", order_num=50,
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

    @property
    def is_sub_pages_visible(self):
        if self.get_et_field("state") in ['source_was_preprocessed', 'sample_classes_assigned', 'ready', 'done']:
            return True
        return False

    def reset(self, *args, **kwargs):
        pass

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

            #self.do_action("success", exp)
            self.enqueue_action("success", (), {})
        except Exception as e:
            ex_type, ex, tb = sys.exc_info()
            traceback.print_tb(tb)
            self.enqueue_action("error", (e,), {})
            #self.do_action("error", exp, e)
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
        log.debug("successfully processed files")


class UploadInteraction(GenericUploadBlock):
    block_base_name = "GENE_INTERACTION"
    name = "Upload gene interaction"

    _block_actions = common_save_actions

    upload_interaction = ParamField(name="upload_interaction", title="Interaction matrix", order_num=10,
        input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM)
    row_units = ParamField(name="row_units", title="Row units",
        order_num=11, input_type=InputType.TEXT, field_type=FieldType.STR, required=False)
    col_units = ParamField(name="col_units", title="Column units",
                           order_num=12, input_type=InputType.TEXT, field_type=FieldType.STR, required=False)

    _interaction = OutputBlockField(name="interaction", provided_data_type="BinaryInteraction")

    def on_params_is_valid(self, exp, *args, **kwargs):
        # Convert to  BinaryInteraction
        interaction_df = self.upload_interaction.get_as_data_frame()

        interaction = BinaryInteraction(exp.get_data_folder(), str(self.uuid))
        interaction.store_matrix(interaction_df)

        interaction.row_units = self.row_units
        interaction.col_units = self.col_units

        self.set_out_var("interaction", interaction)
        exp.store_block(self)


class UploadGeneSets(GenericUploadBlock):
    block_base_name = "GENE_SETS_UPLOAD"
    name = "Upload gene sets"

    _block_actions = common_save_actions

    upload_gs = ParamField(
        name="upload_gs", title="Gene sets in .gmt format", order_num=10,
        input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM
    )

    set_units = ParamField(name="set_units", title="Set units",
                           order_num=11, input_type=InputType.TEXT, field_type=FieldType.STR, required=False)
    gen_units = ParamField(name="gen_units", title="Gene units",
                           order_num=12, input_type=InputType.TEXT, field_type=FieldType.STR, required=False)

    _gene_sets = OutputBlockField(name="gene_sets", provided_data_type="GeneSets")

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
