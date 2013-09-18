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


@task(name='worflow.wrappers.pca_test')
def pca_test(ctx):
    importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
    exp = Experiment.objects.get(e_id = ctx['exp_id'])

    samples_num = ctx['samples_num'] + 1

    tmp = (R.r['iris']).rx(R.IntVector(range(1,samples_num)), R.IntVector(range(1,4)))
    data_set = R.r['t'](tmp)
    cl = (R.r['iris']).rx(R.IntVector(range(1,samples_num)), 5)

    pca = R.r['analysis_pca'](data_set, cl)

    points = pca.do_slot('points')
    R.r['write.table'](points, exp.get_data_file_path(ctx['filename']))

    result = {}
    result['graph_2d_scatter_plot_filename'] = ctx['filename']
    result['graph_name'] = pca.do_slot('main')[0]
    result['graph_caption'] = pca.do_slot('caption')[0]

    ctx.update({"result": result})
    return ctx


@task(name='worflow.wrappers.svm_test')
def svm_test(ctx):
    importr("miXGENE", lib_loc=R_LIB_CUSTOM_PATH)
    exp = Experiment.objects.get(e_id = ctx['exp_id'])

    samples_num = ctx['samples_num'] + 1

    tmp = (R.r['iris']).rx(R.IntVector(range(1,samples_num)), R.IntVector(range(1,4)))
    data_set = R.r['t'](tmp)
    cl = (R.r['iris']).rx(R.IntVector(range(1,samples_num)), 5)

    svm = R.r['linsvm'](data_set, cl)


    result = {}
    result['model'] = str(svm.do_slot('model')[0])

    # predicted is FactorVector, so some magic
    predicted_raw = svm.do_slot('predicted')
    predicted_levels = predicted_raw.levels
    predicted = [
        (str(sample), str(predicted_levels[level_id - 1]))
        for sample, level_id in
        predicted_raw.iteritems()
    ]



    R.r['write.table'](predicted_raw, exp.get_data_file_path(ctx['filename']), row_names=True, col_names=True)
    result['svm_factors_filename'] = ctx['filename']

    result['predicted'] = []
    result['acc'] = int(svm.do_slot('acc')[0])

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

