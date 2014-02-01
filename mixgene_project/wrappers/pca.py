import pandas as pd
import rpy2.robjects as R
from rpy2.robjects.packages import importr
import rpy2.robjects.numpy2ri as rpyn
import pandas.rpy.common as com

from celery import task

from environment.structures import ExpressionSet, PcaResult
from mixgene.settings import R_LIB_CUSTOM_PATH


@task(name='worflow.wrappers.pca_test')
def pca_test(exp, block, es):
    try:
        importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
        assert isinstance(es, ExpressionSet)
        dataset = R.r['new']('mixData')
        r_data = com.convert_to_r_matrix(es.get_assay_data_frame())
        dataset.do_slot_assign('data', r_data)

        dataset_factor = R.r.new('mixPheno')
        pheno_df = es.get_pheno_data_frame()
        r_phenotype = R.r.factor(R.StrVector(pheno_df['Sample_title'].tolist()))
        dataset_factor.do_slot_assign("phenotype", r_phenotype)

        pca = R.r['mixPca'](
            dataset=dataset,
            dataset_factor=dataset_factor,
            )

        r_points = pca.do_slot('points')
        np_points = rpyn.ri2numpy(r_points)
        df_points = pd.DataFrame(np_points)
        df_points.index = pheno_df.index
        res = PcaResult(
            base_dir=exp.get_data_folder(),
            base_filename= "%s_pca" % block.uuid
        )
        res.store_pca(df_points)

        block.pca_result = res
        block.do_action("success", exp)
    except Exception, e:
        block.errors.append(e)
        block.do_action("error", exp)