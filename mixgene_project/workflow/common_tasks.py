from celery import task

from mixgene.util import prepare_GEO_ftp_url, fetch_file_from_url
#from webapp.models import Experiment

@task(name="workflow.common_tasks.fetch_GEO_gse_matrix")
def fetch_GEO_gse_matrix(exp, dataset_var, uid, dir_path):
    """
        @exp: webapp.models.Experiment
        download, unpack and clean matrix file
    """
    url, compressed_filename, filename = prepare_GEO_ftp_url(uid, "GSE", "matrix")
    fetch_file_from_url(url, "%s/%s" % (dir_path, compressed_filename) )

    ctx = exp.get_ctx()
    ctx[dataset_var] = filename
    ctx["exp_fetching_data_var"].remove(dataset_var)
    exp.update_ctx(ctx)
