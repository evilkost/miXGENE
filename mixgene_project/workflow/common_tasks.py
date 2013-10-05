from celery import task

from mixgene.util import prepare_GEO_ftp_url, fetch_file_from_url, clean_GEO_file
#from webapp.models import Experiment

from Bio.Geo import parse as parse_geo
from pandas import Series, DataFrame
from webapp.models import Experiment
from workflow.vars import MixData, MixPheno


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
    fi.filepath = exp.get_data_file_path(target_filename)
    fi.geo_uid = geo_uid
    fi.file_format = file_format
    fi.set_file_type("ncbi_geo")

    exp.update_ctx(ctx)
    exp.validate(None)

@task(name="workflow.common_tasks.soft_to_r_objects")
def soft_to_r_objects(ctx):
    """
        Produce symbols and phenotype R objects( MixData, MixPheno) from .soft file.
        Exptected
    """
    exp = Experiment.objects.get(e_id=ctx["exp_id"])
    soft_var_name = ctx["dataset_var"]
    symbols_var_name = ctx["symbols_var"]
    phenotype_var_name = ctx["phenotype_var"]

    soft_file_input = ctx["input_vars"][soft_var_name]

    if soft_file_input.file_format != "soft":
        raise Exception("Input file %s isn't in SOFT format" % soft_var_name)

    #TODO: now we assume that we get GSE file

    soft = list(parse_geo(open(soft_file_input.filepath)))
    assert soft[2].entity_type == "PLATFORM"

    pl = soft[2].table_rows
    id_idx = pl[0].index('ID')
    entrez_idx = pl[0].index('ENTREZ_GENE_ID')
    mapping = dict([(row[id_idx], row[entrez_idx]) for row in pl[1:]])

    id_ref_idx = soft[3].table_rows[0].index("ID_REF")
    value_idx = soft[3].table_rows[0].index("VALUE")
    df = DataFrame(dict([
        (
            soft[i].entity_attributes['Sample_geo_accession'],
            Series(dict([(row[id_ref_idx], row[value_idx]) for row in soft[i].table_rows[1:]]))
        )
        for i in range(3, len(soft))
    ]))

    symbols_var = MixData()
    symbols_var.filename = "%s_symbols.csv" % soft_var_name
    symbols_var.filepath = exp.get_data_file_path(symbols_var.filename)

    df.to_csv(symbols_var.filepath, sep=" ", index_label=False)

    ctx[symbols_var_name] = symbols_var

    factors = [soft[i].entity_attributes['Sample_title'].split(',')
               for i in range(3, len(soft))]

    df2 = DataFrame({"x": Series(factors)})

    phenotype_var = MixPheno()
    phenotype_var.filename = "%s_pheno.csv" % soft_var_name
    phenotype_var.filepath = exp.get_data_file_path(phenotype_var.filename)

    ctx[phenotype_var_name] = phenotype_var
    df2.to_csv(phenotype_var.filepath, sep=" ", index_label=False)
    return ctx
