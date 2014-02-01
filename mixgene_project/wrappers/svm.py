import traceback
import sys
import rpy2.robjects as R
from rpy2.robjects.packages import importr
import pandas.rpy.common as com

from celery import task

from environment.structures import ExpressionSet, mixML
from mixgene.settings import R_LIB_CUSTOM_PATH

@task(name='worflow.wrappers.svm_test')
def svm_test(exp, block, train, test):
    """
        @type train: ExpressionSet
        @type  test: ExpressionSet
    """
    try:
        importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)

        assay_train_df = train.get_assay_data_frame()
        pheno_train_df = train.get_pheno_data_frame()

        assay_test_df = test.get_assay_data_frame()
        pheno_test_df = test.get_pheno_data_frame()

        rnew = R.r["new"]
        dataset_train = rnew("mixData")
        dataset_test = rnew("mixData")

        dataset_train.do_slot_assign("data", com.convert_to_r_dataframe(assay_train_df))
        dataset_test.do_slot_assign("data", com.convert_to_r_dataframe(assay_test_df))

        pheno_train = rnew("mixPheno")
        pheno_test = rnew("mixPheno")

        pheno_train.do_slot_assign("phenotype",
                                   R.r.factor(R.StrVector(pheno_train_df['User_class'].tolist()))
        )
        pheno_test.do_slot_assign("phenotype",
                                  R.r.factor(R.StrVector(pheno_test_df['User_class'].tolist()))
        )

        svm = R.r['mixSvmLin'](
            dataset=dataset_train,
            dataset_factor=pheno_train,

            new_dataset=dataset_test,
            new_dataset_factor=pheno_test,
            )

        result = mixML(exp, svm, "%s_ML" % block.uuid)
        result.title = "SVM result"

        block.mixMlResult = result

        block.do_action("on_svm_done", exp)
    except Exception, e:
        ex_type, ex, tb = sys.exc_info()
        traceback.print_tb(tb)
        print e
        #TODO: LOG ERROR AND TRACEBACK OR WE LOSE IT!
        #import ipdb; ipdb.set_trace()
        block.errors.append(e)
        block.do_action("on_svm_error", exp)