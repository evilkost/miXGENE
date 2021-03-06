import rpy2.robjects as R
import pandas.rpy.common as com

from celery import task

from converters.gene_set_tools import filter_gs_by_genes
from environment.structures import TableResult

import sys
import traceback


class GlobalTest(object):
    gt = None

    @staticmethod
    def gt_init():
        if GlobalTest.gt is None:
            # importr("globaltest", lib_loc=R_LIB_CUSTOM_PATH)
            R.r['library']("globaltest")  # lib_loc=R_LIB_CUSTOM_PATH)
            GlobalTest.gt = R.r['gt']

    @staticmethod
    def gt_basic(es, gene_sets, pheno_class_column,
                 model="logistic",
                 permutations=100):
        """
            @param es: Expression set with defined user class in pheno
            @type es: ExpressionSet

            @type gene_sets: environment.structures.GeneSets

            @param pheno_class_column: Column name of target classes in phenotype table
            @type pheno_class_column: string or None
        """
        GlobalTest.gt_init()

        dataset = com.convert_to_r_matrix(es.get_assay_data_frame())
        response = es.get_pheno_column_as_r_obj(pheno_class_column)

        genes_in_es = es.get_assay_data_frame().index.tolist()
        gs_filtered = filter_gs_by_genes(gene_sets.get_gs(), genes_in_es)

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


# @task(name="wrappers.gt.global_test_task")
def global_test_task(
        exp, block,
        es, gene_sets,
        # base_dir, base_filename,
        table_result
    ):
    """
    @param es: Expression set with defined user class in pheno
    @type es: ExpressionSet

    @type gs: environment.structures.GeneSets

    @param filepath: Fully qualified filepath to store result data frame
    @type filepath: str

    @param pheno_class_column: Column name of target classes in phenotype table
    @type pheno_class_column: str or None
    """

    result_df = GlobalTest.gt_basic(es, gene_sets, es.pheno_metadata["user_class_title"])
    table_result.store_table(result_df)
    return [table_result], {}

