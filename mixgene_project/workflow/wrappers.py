from collections import defaultdict
from uuid import uuid1

from celery import task

import rpy2.robjects as R
from rpy2.robjects.packages import importr

from mixgene.settings import R_LIB_CUSTOM_PATH
from webapp.models import Experiment, UploadedData


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

class mixPlot(object):
    def __init__(self, exp, rMixPlot, csv_filename):
        self.uuid = str(uuid1())
        self.template = "workflow/result/mixPlot.html"
        self.title = "mixPlot"

        self.main = rMixPlot.do_slot('main')[0]
        self.caption = rMixPlot.do_slot('caption')[0]

        # haven't implimented .do_slot('cl') since points already has this data
        self.filename = csv_filename
        self.filepath = exp.get_data_file_path(csv_filename)

        R.r['write.table'](rMixPlot.do_slot('points'), self.filepath, row_names=True, col_names=True)

        self.has_col_names = True
        self.has_row_names = True
        self.csv_delimiter = " "


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

class mixML(object):
    def __init__(self, exp, rMixML, csv_filename):
        self.uuid = str(uuid1())
        self.template = "workflow/result/mixML.html"
        self.title = "mixML"

        self.model = str(rMixML.do_slot('model')[0])
        self.acc = int(rMixML.do_slot('acc')[0])
        self.working_units = list(rMixML.do_slot('working.units'))

        predicted = rMixML.do_slot('predicted')

        self.filename = csv_filename
        self.filepath = exp.get_data_file_path(csv_filename)

        R.r['write.table'](predicted, self.filepath, row_names=True, col_names=True)

        self.has_col_names = True
        self.has_row_names = True
        self.csv_delimiter = " "


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


class mixTable(object):
    def __init__(self, exp, rMixTable, csv_filename):
        self.uuid = str(uuid1())
        self.template = "workflow/result/mixTable.html"
        self.title = "mixTable"

        self.caption = rMixTable.do_slot('caption')[0]
        self.working_units = list(rMixTable.do_slot('working.units'))

        self.filename = csv_filename
        self.filepath = exp.get_data_file_path(csv_filename)
        rMixTable.do_slot('table').to_csvfile(self.filepath, sep=" ")

        self.has_col_names = True
        self.has_row_names = True
        self.csv_delimiter = " "


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