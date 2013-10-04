from logilab.common.graph import target_info_from_filename
from celery import task

from mixgene.util import prepare_GEO_ftp_url, fetch_file_from_url, clean_GEO_file
#from webapp.models import Experiment


@task(name="workflow.common_tasks.fetch_GEO_gse_matrix")
def fetch_geo_gse(exp, var_name, geo_uid, file_format):
    """
        @exp: webapp.models.Experiment
        download, unpack and clean matrix file
    """
    dir_path = exp.get_data_folder()

    url, compressed_filename, filename = prepare_GEO_ftp_url(geo_uid, file_format)
    fetch_file_from_url(url, "%s/%s" % (dir_path, compressed_filename))


    if file_format == "txt":
        target_filename = "%s.clean.csv" % filename
        clean_GEO_file(exp.get_data_file_path(filename),
                       exp.get_data_file_path(target_filename))
    else:
        target_filename = filename

    ctx = exp.get_ctx()

    fi = ctx["input_vars"][var_name]
    fi.is_done = True
    fi.is_being_fetched = False
    fi.filename = target_filename
    fi.geo_uid = geo_uid
    fi.file_format = file_format
    fi.set_file_type("ncbi_geo")

    exp.update_ctx(ctx)
    exp.validate(None)
