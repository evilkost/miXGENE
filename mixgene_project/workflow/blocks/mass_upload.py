# -*- coding: utf-8 -*-

import json
import logging
import traceback
import sys

import pandas as pd

from environment.structures import ExpressionSet, BinaryInteraction, prepare_phenotype_for_js_from_es
from workflow.blocks.generic import GenericBlock, ActionsList, BlockField, FieldType, \
    ActionRecord, ParamField, InputType, OutputBlockField, InnerOutputField
from workflow.blocks.meta_block import UniformMetaBlock

from workflow.blocks.fields.upload import MultiUploadField

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

class MassUpload(UniformMetaBlock):
    block_base_name = "BunchUpload"
    _bu_block_actions = ActionsList([

        ActionRecord("process_upload", ["valid_params", "processing_upload"],
                     "processing_upload", "Process uploaded data"),

        ActionRecord("error_on_processing", ["processing_upload"], "valid_params"),
        ActionRecord("processing_done", ["processing_upload"], "ready")

        # ActionRecord("success", ["processing_upload"], "done", reload_block_in_client=True),
        # ActionRecord("error", ["processing_upload"], "valid_params"),
    ])

    es_matrices = ParamField(
        "es_matrices", title="Expression sets", order_num=10,
        input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM,
        options={"multiple": True},
    )

    pheno_matrices = ParamField(
        "pheno_matrices", title="Phenotypes", order_num=40,
        input_type=InputType.FILE_INPUT, field_type=FieldType.CUSTOM,
        options={"multiple": True},
    )

    def __init__(self, *args, **kwargs):
        super(MassUpload, self).__init__("User upload", *args, **kwargs)
        self.es_matrices = MultiUploadField()
        self.pheno_matrices = MultiUploadField()

        self.pheno_by_es_names = {}
        self.labels = []
        self.seq = []

        self.register_inner_output_variables([InnerOutputField(
            name="es",
            provided_data_type="ExpressionSet"
        )])

    @property
    def is_sub_pages_visible(self):
        if self.state in ['source_was_preprocessed', 'sample_classes_assigned', 'ready', 'done']:
            return True
        return False

    def get_fold_labels(self):
        return self.labels

    def error_on_processing(self, *args, **kwargs):
        pass

    def processing_done(self, *args, **kwargs):
        pass

    def process_upload(self, exp, *args, **kwargs):
        """
            @param exp: Experiment
        """
        # TODO: move to celery
        self.clean_errors()
        seq = []

        try:
            if len(self.pheno_matrices) != len(self.es_matrices):
                raise RuntimeError("Different number of phenotypes and expression sets")

            self.labels = es_matrix_names = sorted(self.es_matrices)
            pheno_matrix_names = sorted(self.pheno_matrices)
            self.pheno_by_es_names = {
                es_name: pheno_name for
                es_name, pheno_name
                in zip(es_matrix_names, pheno_matrix_names)
            }
            for es_name, pheno_name in self.pheno_by_es_names.iteritems():
                es_ufw = self.es_matrices[es_name]
                es_df = es_ufw.get_as_data_frame()

                pheno_ufw = self.pheno_matrices[pheno_name]
                pheno_df = pheno_ufw.get_as_data_frame()

                es_sample_names = sorted(es_df.columns.tolist())
                pheno_sample_names = sorted(pheno_df.index.tolist())
                if es_sample_names != pheno_sample_names:
                    raise RuntimeError("Couldn't match `%s` and `%s` due to "
                                       "different sample name sets" % (es_name, pheno_name))

                es = ExpressionSet(
                    base_dir=exp.get_data_folder(),
                    base_filename="%s_%s" % (self.uuid, es_name)
                )
                es.store_assay_data_frame(es_df)
                es.store_pheno_data_frame(pheno_df)

                es.pheno_metadata["user_class_title"] = pheno_df.columns[0]
                seq.append({"es": es, "__label__": es_name})

            self.seq = seq
            exp.store_block(self)
            self.do_action("processing_done", exp, seq)
        except Exception as e:
            log.exception(e)
            self.do_action("error_on_processing", exp, e)
            # self.celery_task_fetch.apply_async()

    def execute(self, exp, *args, **kwargs):
        self.inner_output_manager.reset()
        self.do_action("on_folds_generation_success", exp, self.seq)

    def success(self, exp, *args, **kwargs):
        pass