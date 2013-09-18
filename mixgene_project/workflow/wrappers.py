from collections import defaultdict

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
        self.main = rMixPlot.do_slot('main')[0]
        self.caption = rMixPlot.do_slot('caption')[0]

        # haven't implimented .do_slot('cl') since points already has this data
        self.filename = csv_filename
        self.filepath = exp.get_data_file_path(csv_filename)

        R.r['write.table'](rMixPlot.do_slot('points'), self.filepath, row_names=True, col_names=True)

        self.has_col_names = True
        self.has_row_names = True


@task(name='worflow.wrappers.pca_test')
def pca_test(ctx):
    importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
    exp = Experiment.objects.get(e_id = ctx['exp_id'])

    samples_num = ctx['samples_num'] + 1

    tmp = (R.r['iris']).rx(R.IntVector(range(1,samples_num)), R.IntVector(range(1,4)))
    data_set = R.r['t'](tmp)
    cl = (R.r['iris']).rx(R.IntVector(range(1,samples_num)), 5)

    pca = R.r['analysis_pca'](data_set, cl)

    result = mixPlot(exp, pca, ctx['filename'])
    ctx.update({"result": result})
    return ctx

class mixML(object):
    def __init__(self, exp, rMixML, csv_filename):
        self.model = str(rMixML.do_slot('model')[0])
        self.acc = int(rMixML.do_slot('acc')[0])
        self.working_units = list(rMixML.do_slot('working.units'))

        predicted = rMixML.do_slot('predicted')

        self.filename = csv_filename
        self.filepath = exp.get_data_file_path(csv_filename)

        R.r['write.table'](predicted, self.filepath, row_names=True, col_names=True)

        self.has_col_names = True
        self.has_row_names = True


@task(name='worflow.wrappers.svm_test')
def svm_test(ctx):
    importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
    exp = Experiment.objects.get(e_id = ctx['exp_id'])
    samples_num = ctx['samples_num'] + 1
    tmp = (R.r['iris']).rx(R.IntVector(range(1,samples_num)), R.IntVector(range(1,4)))
    data_set = R.r['t'](tmp)
    cl = (R.r['iris']).rx(R.IntVector(range(1,samples_num)), 5)

    svm = R.r['linsvm'](data_set, cl)

    result = mixML(exp, svm, ctx['filename'])
    ctx.update({"result": result})
    return ctx


class mixTable(object):
    def __init__(self, exp, rMixTable, csv_filename):
        self.caption = rMixTable.do_slot('caption')[0]
        self.working_units = list(rMixTable.do_slot('working.units'))

        self.filename = csv_filename
        self.filepath = exp.get_data_file_path(csv_filename)
        rMixTable.do_slot('table').to_csvfile(self.filepath, sep=" ")

        self.has_col_names = True
        self.has_row_names = True


@task(name='worflow.wrappers.tt_test')
def tt_test(ctx):
    importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
    exp = Experiment.objects.get(e_id = ctx['exp_id'])

    samples_num = ctx['samples_num'] + 1

    tmp = (R.r['iris']).rx(R.IntVector(range(1,samples_num)), R.IntVector(range(1,4)))
    data_set = R.r['t'](tmp)
    cl = (R.r['iris']).rx(R.IntVector(range(1,samples_num)), 5)

    tt = R.r['ttest'](data_set, cl)

    result = mixTable(exp, R.r['ttest'](data_set, cl), ctx['filename'])

    ctx.update({"result": result})
    return ctx

