from celery import task

import rpy2.robjects as R
from rpy2.robjects.packages import importr

from mixgene.settings import R_LIB_CUSTOM_PATH
from webapp.models import Experiment
from workflow.result import mixPlot, mixML, mixTable


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


@task(name='worflow.wrappers.pca_test')
def pca_test(ctx):
    importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
    R.r['options'](warn=-1)

    exp = Experiment.objects.get(e_id=ctx['exp_id'])

    rdata = R.r['data']
    rdata('leukemia.symbols')
    rdata('leukemia.pheno')
    rdata('msigdb.symbols')

    #pca.results <- mixPca(leukemia.symbols, leukemia.pheno, center = T, scale = F)
    rMPCA = R.r['mixPca']
    pca = rMPCA(dataset=R.r['leukemia.symbols'], dataset_factor=R.r['leukemia.pheno'])

    result = mixPlot(exp, pca, ctx['filename'])
    result.title = "PCA test"
    ctx.update({"result": result})
    return ctx


@task(name='worflow.wrappers.svm_test')
def svm_test(ctx):
    importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
    R.r['options'](warn=-1)

    exp = Experiment.objects.get(e_id=ctx['exp_id'])

    rdata = R.r['data']
    rdata('leukemia.symbols')
    rdata('leukemia.pheno')
    rdata('msigdb.symbols')

    rMSVML = R.r['mixSvmLin']
    svm = rMSVML(dataset=R.r['leukemia.symbols'], dataset_factor=R.r['leukemia.pheno'])
    result = mixML(exp, svm, ctx['filename'])

    #svmlin.results <- mixSvmLin(dataset=leukemia.symbols, dataset.factor=leukemia.pheno)

    result.title = "SVM result"
    ctx.update({"result": result})
    return ctx


@task(name='worflow.wrappers.tt_test')
def tt_test(ctx):
    importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
    R.r['options'](warn=-1)

    exp = Experiment.objects.get(e_id = ctx['exp_id'])

    rdata = R.r['data']
    rdata('leukemia.symbols')
    rdata('leukemia.pheno')
    rdata('msigdb.symbols')


    # ttest.results <- mixTtest(dataset=leukemia.symbols, dataset.factor=leukemia.pheno)
    rMTT = R.r['mixTtest']
    raw_res = rMTT(dataset=R.r['leukemia.symbols'], dataset_factor=R.r['leukemia.pheno'])
    result = mixTable(exp, raw_res, ctx['filename'])
    result.title = "T-test"
    ctx.update({"result": result})
    return ctx

@task(name='workflow.wrappers.mix_global_test')
def mix_global_test(ctx):
    importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
    R.r['options'](warn=-1)

    exp = Experiment.objects.get(e_id = ctx['exp_id'])

    rdata = R.r['data']
    rdata('leukemia.symbols')
    rdata('leukemia.pheno')
    rdata('msigdb.symbols')


    rMGT = R.r['mixGlobaltest']
    raw_res = rMGT(dataset=R.r['leukemia.symbols'], dataset_factor=R.r['leukemia.pheno'], gene_sets=R.r['msigdb.symbols'])

    result = mixTable(exp, raw_res, ctx['filename'])
    result.title = "Global test"
    ctx.update({"result": result})
    return ctx