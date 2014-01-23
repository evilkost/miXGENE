import traceback
from celery import task

import rpy2.rinterface
import rpy2.robjects as R
from rpy2.robjects.packages import importr
import pandas as pd
import pandas.rpy.common as com
import rpy2.robjects.numpy2ri as rpyn
import sys

from mixgene.settings import R_LIB_CUSTOM_PATH

from environment.structures import ExpressionSet, PcaResult

from webapp.models import Experiment
from workflow.result import mixPlot, mixML, mixTable
from workflow.vars import MixData, MixPheno


R.r['options'](warn=-1)

@task(name='workflow.wrappers.r_test_algo')
def r_test_algo(ctx):
    importr("test", lib_loc=R_LIB_CUSTOM_PATH)
    exp = Experiment.objects.get(e_id = ctx['exp_id'])

    filename = ctx["data.csv"]

    rread_csv = R.r['read.csv']
    rwrite_csv = R.r['write.csv']
    rtest = R.r['test']

    rx = rread_csv(filename)
    rres = rtest(rx)

    names_to_res = ['sum', 'nrow', 'ncol',]
    for i in range(len(rres.names)):
        if rres.names[i] in names_to_res:
            ctx[rres.names[i]] = rres[i][0]

    return ctx


@task(name='workflow.wrappers.leukemia_data_provider')
def leukemia_data_provider(ctx):
    importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
    R.r['options'](warn=-1)

    exp = Experiment.objects.get(e_id=ctx['exp_id'])

    r_data = R.r['data']
    r_data('leukemia.symbols')
    r_data('leukemia.pheno')
    rls = R.r['leukemia.symbols']
    rlp = R.r['leukemia.pheno']

    leukemia_symbols = MixData()
    leukemia_pheno = MixPheno()

    leukemia_symbols.filename = "leukemia.symbols"
    leukemia_symbols.filepath = exp.get_data_file_path(leukemia_symbols.filename)

    leukemia_symbols.org = list(rls.do_slot("org"))
    leukemia_symbols.units = list(rls.do_slot("units"))

    R.r['write.table'](rls.do_slot('data'), leukemia_symbols.filepath,
                       row_names=leukemia_symbols.has_row_names,
                       col_names=leukemia_symbols.has_col_names)

    leukemia_pheno.filename = "leukemia.pheno"
    leukemia_pheno.filepath = exp.get_data_file_path(leukemia_pheno.filename)


    leukemia_pheno.org = list(rlp.do_slot("org"))
    leukemia_pheno.units = list(rlp.do_slot("units"))

    R.r['write.table'](rlp.do_slot('phenotype'), leukemia_pheno.filepath,
                       row_names=leukemia_pheno.has_row_names,
                       col_names=leukemia_pheno.has_col_names)

    ctx["leukemia_symbols"] = leukemia_symbols
    ctx["leukemia_phenotype"] = leukemia_pheno
    return ctx


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


@task(name='worflow.wrappers.tt_test')
def tt_test(ctx):
    importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
    exp = Experiment.objects.get(e_id=ctx['exp_id'])

    tt = R.r['mixTtest'](
        dataset=ctx["expression"].to_r_obj(),
        dataset_factor=ctx["phenotype"].to_r_obj(),
    )
    result = mixTable(exp, tt, ctx['filename'])
    result.title = "T-test"
    ctx.update({"result": result})
    return ctx

@task(name='workflow.wrappers.mix_global_test')
def mix_global_test(ctx):
    importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
    exp = Experiment.objects.get(e_id=ctx['exp_id'])

    R.r["sink"]("aux")
    global_test = R.r['mixGlobaltest'](
        dataset=ctx["expression"].to_r_obj(),
        dataset_factor=ctx["phenotype"].to_r_obj(),
        gene_sets=ctx["gene_sets"].to_r_obj(),
    )
    R.r["sink"](rpy2.rinterface.NULL)
    result = mixTable(exp, global_test, ctx['filename'])
    result.title = "Global test"
    ctx.update({"result": result})
    return ctx

@task(name='workflow.wrappers.pca_agg')
def pca_agg(ctx):
    importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
    #tmp <- pcaAgg(gene.sets,
    #   train.dataset, test.dataset)

    exp = Experiment.objects.get(e_id=ctx['exp_id'])

    gene_sets = ctx["gene_sets"]

    aggregated = R.r['pcaAgg'](
        gene_sets=ctx["gene_sets"].to_r_obj(),
        train_dataset=ctx["expression_train"].to_r_obj(),
        test_dataset=ctx["expression_test"].to_r_obj(),
    )

    #TODO: create new variables

    R.r['write.table'](aggregated.do_slot("training.dataset").do_slot("data"),
        ctx["expression_train"].filepath, row_names=True, col_names=True)
    R.r['write.table'](aggregated.do_slot("testing.dataset").do_slot("data"),
        ctx["expression_test"].filepath, row_names=True, col_names=True)

    return ctx


