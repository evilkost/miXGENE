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

    samples_num = ctx['samples_num']

    tmp = (R.r['iris']).rx(R.IntVector(range(1,samples_num)), R.IntVector(range(1,4)))
    data_set = R.r['t'](tmp)
    cl = (R.r['iris']).rx(R.IntVector(range(1,samples_num)), 5)

    pca = R.r['analysis_pca'](data_set, cl)

    points = pca.do_slot('points')
    R.r['write.table'](points, exp.get_data_file_path(ctx['points_filename']))

    pca_result = {}
    pca_result['graph_2d_scatter_plot_filename'] = ctx['points_filename']
    pca_result['graph_name'] = pca.do_slot('main')[0]
    pca_result['graph_caption'] = pca.do_slot('caption')[0]

    ctx.update({"pca_result": pca_result})
    return ctx
