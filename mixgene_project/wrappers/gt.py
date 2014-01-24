import traceback

import pandas as pd
import rpy2.robjects as R
from rpy2.robjects.packages import importr
import pandas.rpy.common as com

from celery import task

from converters.gene_set_tools import filter_gs_by_genes
from environment.structures import DataFrameStorage
from mixgene.settings import R_LIB_CUSTOM_PATH

import sys
import traceback


class GlobalTest(object):
    gt = None

    @staticmethod
    def gt_init():
        if GlobalTest.gt is None:
            #importr("globaltest", lib_loc=R_LIB_CUSTOM_PATH)
            R.r['library']("globaltest") #, lib_loc=R_LIB_CUSTOM_PATH)
            GlobalTest.gt = R.r['gt']

    @staticmethod
    def gt_basic(es, gs, pheno_class_column="User_class",
                 model="logistic",
                 permutations=100):
        """
            @param es: Expression set with defined user class in pheno
            @type es: ExpressionSet

            @type gs: environment.structures.GeneSets

            @param pheno_class_column: Column name of target classes in phenotype table
            @type pheno_class_column: string or None
        """
        GlobalTest.gt_init()

        dataset = com.convert_to_r_matrix(es.get_assay_data_frame())
        response = es.get_pheno_column_as_r_obj(pheno_class_column)

        genes_in_es = es.get_assay_data_frame().index.tolist()
        gs_filtered = filter_gs_by_genes(gs, genes_in_es)

        gt_instance = GlobalTest.gt(
            response,
            R.r['t'](dataset),
            subsets=gs_filtered.to_r_obj(),
            model=model,
            permutations=permutations,
        )

        result = gt_instance.do_slot('result')
        result_df = com.convert_robj(result)
        return result_df


@task(name="wrappers.gt.global_test_task")
def global_test_task(
        exp, block, store_field,
        es, gs_storage,
        filepath,
        pheno_class_column="User_class",
        success_action="success", error_action="error"
    ):
    """
    @param es: Expression set with defined user class in pheno
    @type es: ExpressionSet

    @type gs: environment.structures.GmtStorage

    @param filepath: Fully qualified filepath to store result dataframe
    @type filepath: str


    @param pheno_class_column: Column name of target classes in phenotype table
    @type pheno_class_column: str or None
    """
    try:
        gs = gs_storage.load()
        result_df = GlobalTest.gt_basic(es, gs, pheno_class_column)
        result_storage = DataFrameStorage(filepath)
        result_storage.store(result_df)

        setattr(block, store_field, result_storage)
        block.do_action(success_action, exp)
    except Exception, e:
        ex_type, ex, tb = sys.exc_info()
        traceback.print_tb(tb)
        block.errors.append(e)
        block.do_action(error_action, exp)
