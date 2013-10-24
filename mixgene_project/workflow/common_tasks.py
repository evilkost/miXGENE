from collections import defaultdict
import shutil
import random
from celery import task
from pandas import Series, DataFrame

from Bio.Geo import parse as parse_geo

from mixgene.util import prepare_GEO_ftp_url, fetch_file_from_url, clean_GEO_file, transpose_dict_list
from webapp.models import Experiment, CachedFile
from workflow.parsers import GMT
from workflow.vars import MixData, MixPheno, GeneSets


@task(name="workflow.common_tasks.fetch_GEO_gse_matrix")
def fetch_geo_gse(exp, var_name, geo_uid, file_format):
    """
        @exp: webapp.models.Experiment
        download, unpack and clean matrix file
    """
    dir_path = exp.get_data_folder()

    url, compressed_filename, filename = prepare_GEO_ftp_url(geo_uid, file_format)

    mb_cached = CachedFile.look_up(url)
    if mb_cached is None:
        fetch_file_from_url(url, "%s/%s" % (dir_path, compressed_filename))

        if file_format == "txt":
            target_filename = "%s.clean.csv" % filename
            clean_GEO_file(exp.get_data_file_path(filename),
                           exp.get_data_file_path(target_filename))
            filename = target_filename

        CachedFile.update_cache(url, exp.get_data_file_path(filename))
    else:
        shutil.copy(mb_cached.get_file_path(), exp.get_data_file_path(filename))
        print "copied file from cache"

    ctx = exp.get_ctx()

    fi = ctx["input_vars"][var_name]
    fi.is_done = True
    fi.is_being_fetched = False
    fi.filename = filename
    fi.filepath = exp.get_data_file_path(filename)
    fi.geo_uid = geo_uid
    fi.geo_type = geo_uid[:3]

    fi.file_format = file_format
    fi.set_file_type("ncbi_geo")

    exp.update_ctx(ctx)
    exp.validate(None)

@task(name="workflow.common_tasks.preprocess_soft")
def preprocess_soft(ctx):
    """
        Produce symbols and phenotype R objects( MixData, MixPheno) from .soft file.
        Exptected
    """
    exp = Experiment.objects.get(e_id=ctx["exp_id"])
    soft_var_name = ctx["dataset_var"]
    expression_var_name = ctx["expression_var"]
    phenotype_var_name = ctx["phenotype_var"]
    genesets_var_name = ctx["gene_sets_var"]

    soft_file_input = ctx["input_vars"][soft_var_name]

    if soft_file_input.file_format != "soft":
        raise Exception("Input file %s isn't in SOFT format" % soft_var_name)

    #TODO: now we assume that we get GSE file

    soft = list(parse_geo(open(soft_file_input.filepath)))
    assert soft[2].entity_type == "PLATFORM"

    pl = soft[2].table_rows
    id_idx = pl[0].index('ID')
    entrez_idx = pl[0].index('ENTREZ_GENE_ID')
    probe_to_genes_mapping = dict([(row[id_idx], row[entrez_idx].split(" /// ")) for row in pl[1:]])

    id_ref_idx = soft[3].table_rows[0].index("ID_REF")
    value_idx = soft[3].table_rows[0].index("VALUE")
    df = DataFrame(dict([
        (
            soft[i].entity_attributes['Sample_geo_accession'],
            Series(dict([(row[id_ref_idx], row[value_idx]) for row in soft[i].table_rows[1:]]))
        )
        for i in range(3, len(soft))
    ]))

    expression_var = MixData()
    expression_var.filename = "%s_expression.csv" % soft_var_name
    expression_var.filepath = exp.get_data_file_path(expression_var.filename)
    expression_var.org = [soft[2].entity_attributes['Platform_organism']]
    expression_var.units = ['probe_ids']

    df.to_csv(expression_var.filepath, sep=" ", index_label=False)

    ctx[expression_var_name] = expression_var

    factors = [soft[i].entity_attributes['Sample_title'].split(',')[0].split('_')[0] ## just for test
               for i in range(3, len(soft))]

    df2 = DataFrame({"x": Series(factors)})

    phenotype_var = MixPheno()
    phenotype_var.filename = "%s_pheno.csv" % soft_var_name
    phenotype_var.filepath = exp.get_data_file_path(phenotype_var.filename)

    phenotype_var.org = expression_var.org
    phenotype_var.units = expression_var.units

    ctx[phenotype_var_name] = phenotype_var
    df2.to_csv(phenotype_var.filepath, sep=" ", index_label=False)

    gmt = GMT()
    gmt.gene_sets = probe_to_genes_mapping
    gmt.description = dict([(key, "") for key in probe_to_genes_mapping])

    gs = GeneSets()
    gs.filename = "%s_gs.gmt" % genesets_var_name
    gs.filepath = exp.get_data_file_path(gs.filename)
    gs.gene_units = "ENTREZ_GENE_ID"
    gs.set_units = "PROBE_ID"
    gmt.write_file(gs.filepath)

    ctx[genesets_var_name] = gs

    return ctx

@task(name="workflow.common_tasks.converse_probes_to_genes")
def converse_probes_to_genes(ctx):
    exp = Experiment.objects.get(e_id=ctx["exp_id"])

    expression_var_name = ctx["expression_var"]
    expression_trans_var_name = ctx["expression_trans_var"]
    genesets_var_name = ctx["gene_sets_var"]

    gmt = ctx[genesets_var_name].get_gmt()
    set_by_gene = transpose_dict_list(gmt)

    expression_var = ctx[expression_var_name]
    src = DataFrame.from_csv(expression_var.filepath, sep=" ")

    res = DataFrame(index=set_by_gene.keys(), columns=src.columns)
    for gen, set_ids in set_by_gene.iteritems():
        cut = src.loc[set_ids]
        res.loc[gen] = cut.mean()

    expression_var_trans = MixData()
    expression_var_trans.org = expression_var.org
    expression_var_trans.units = ["gene_ids"]
    expression_var_trans.filename = "%s_expression_trans.csv" % ctx["dataset_var"]
    expression_var_trans.filepath = exp.get_data_file_path(expression_var_trans.filename)

    res.to_csv(expression_var_trans.filepath, sep=" ", index_label=False)
    ctx[expression_trans_var_name] = expression_var_trans

    return ctx
