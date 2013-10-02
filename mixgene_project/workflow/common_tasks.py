from logilab.common.graph import target_info_from_filename
from celery import task

from webapp.models import FileInput
from mixgene.util import prepare_GEO_ftp_url, fetch_file_from_url, clean_GEO_file
#from webapp.models import Experiment


@task(name="workflow.common_tasks.fetch_GEO_gse_matrix")
def fetch_GEO_gse(exp, var_name, geo_uid, file_format):
    """
        @exp: webapp.models.Experiment
        download, unpack and clean matrix file
    """
    do_clean = False
    if file_format == "matrix":
        do_clean = True

    ctx = exp.get_ctx()
    dir_path = exp.get_data_folder()

    url, compressed_filename, filename = prepare_GEO_ftp_url(geo_uid, file_format)
    fetch_file_from_url(url, "%s/%s" % (dir_path, compressed_filename) )

    if do_clean:
        target_filename = "%s.clean.csv" % filename

        clean_GEO_file(exp.get_data_file_path(filename),
            exp.get_data_file_path(target_filename))
    else:
        target_filename = filename

    ctx["exp_fetching_data_var"].remove(var_name)
    fi = FileInput("ncbi_geo", var_name,
                   target_filename, geo_uid=geo_uid, file_format=file_format)
    fi.is_fetch_done = True
    ctx['exp_file_vars'][var_name] = fi

    exp.update_ctx(ctx)
    exp.validate(None)
