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

        CachedFile.update_cache(url, exp.get_data_file_path(filename[:-5], file_extension="soft"))
    else:
        shutil.copy(mb_cached.get_file_path(), exp.get_data_file_path(filename[:-5], file_extension="soft"))
        print "copied file from cache"

    ctx = exp.get_ctx()

    fi = ctx["input_vars"][var_name]
    fi.is_done = True
    fi.is_being_fetched = False
    fi.file_extension = "soft"
    fi.filename = filename[:-5]  # FIXME: when we will start using gzipped filed
                                 # extenstion will be longer, so need to fix it
                                 # I think, prepare geo should return clean filename
                                 # and we should keep information about gz comprassion
    fi.filepath = exp.get_data_file_path(fi.filename, fi.file_extension)
    fi.geo_uid = geo_uid
    fi.geo_type = geo_uid[:3]

    fi.file_format = file_format
    fi.set_file_type("ncbi_geo")

    exp.update_ctx(ctx)
    exp.validate(None)

@task(name="workflow.common_tasks.split_train_test")
def split_train_test(ctx):
    exp = Experiment.objects.get(e_id=ctx["exp_id"])

    expression = ctx["expression"]
    phenotype = ctx["phenotype"]
    #FIXME:!!!!!!!!!!!!!
    test_split_ratio = ctx["input_vars"]["common_settings"].inputs["test_split_ratio"].value

    pheno_df = phenotype.to_data_frame()
    train_set_names = set(pheno_df.index)
    test_set_names = set()
    for sample_class, rows in pheno_df.groupby('x'):
        num = len(rows)
        idxs = range(num)
        random.shuffle(idxs)

        selected_idxs = idxs[:max(1, int(num*test_split_ratio))]

        for i in selected_idxs:
            test_set_names.add(rows.iloc[i].name)

    train_set_names.difference_update(test_set_names)

    exp_df = expression.to_data_frame()

    df_train = exp_df.loc[:, train_set_names]
    df_test = exp_df.loc[:, test_set_names]

    pheno_train = pheno_df.loc[train_set_names]
    pheno_test = pheno_df.loc[test_set_names]

    expression_train = MixData()
    expression_test = MixData()
    expression_train.copy_meta_from(expression)
    expression_test.copy_meta_from(expression)

    expression_train.filename = expression.filename + "_train"
    expression_test.filename = expression.filename + "_test"
    expression_train.filepath = exp.get_data_file_path(expression_train.filename)
    expression_test.filepath = exp.get_data_file_path(expression_test.filename)
    #FIXME: this should be done inside MixData objects
    df_train.to_csv(expression_train.filepath, sep=expression_train.delimiter, index_label=False)
    df_test.to_csv(expression_test.filepath, sep=expression_train.delimiter, index_label=False)

    phenotype_train = MixPheno()
    phenotype_test = MixPheno()
    phenotype_train.copy_meta_from(phenotype)
    phenotype_test.copy_meta_from(phenotype)

    phenotype_train.filename = phenotype.filename + "_train"
    phenotype_test.filename = phenotype.filename + "_test"
    phenotype_train.filepath = exp.get_data_file_path(phenotype_train.filename)
    phenotype_test.filepath = exp.get_data_file_path(phenotype_test.filename)

    pheno_train.to_csv(phenotype_train.filepath, sep=phenotype_train.delimiter, index_label=False)
    pheno_test.to_csv(phenotype_test.filepath, sep=phenotype_test.delimiter, index_label=False)

    ctx['expression_train'] = expression_train
    ctx['expression_test'] = expression_test
    ctx['phenotype_train'] = phenotype_train
    ctx['phenotype_test'] = phenotype_test
    return ctx

@task(name="workflow.common_tasks.preprocess_soft")
def preprocess_soft(ctx):
    """
        Produce symbols and phenotype objects( MixData, MixPheno) from .soft file.
        Exptected
    """
    exp = Experiment.objects.get(e_id=ctx["exp_id"])

    #expression_var_name = ctx["expression_var"]
    #phenotype_var_name = ctx["phenotype_var"]
    #genesets_var_name = ctx["gene_sets_var"]

    soft_file_input = ctx["input_vars"]["dataset"]

    if soft_file_input.file_format != "soft":
        raise Exception("Input file %s isn't in SOFT format" % soft_file_input.filepath)

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
    expression_var.filename = "%s_expression.csv" % soft_file_input.filename
    expression_var.filepath = exp.get_data_file_path(expression_var.filename)
    expression_var.org = [soft[2].entity_attributes['Platform_organism']]
    expression_var.units = ['PROBE_ID']

    df.to_csv(expression_var.filepath, sep=expression_var.delimiter, index_label=False)

    #ctx[expression_var_name] = expression_var
    ctx["expression"] = expression_var


    if "gse_factors" in ctx:
        factors = ctx["gse_factors"]
        factors = dict(filter(lambda (k, v):  v != '', factors.iteritems()))
    else:
        factors = [soft[i].entity_attributes['Sample_title'].split(',')[0].split('_')[0] ## just for test
                   for i in range(3, len(soft))]

    df2 = DataFrame({"x": Series(factors)})

    phenotype_var = MixPheno()
    phenotype_var.filename = "%s_pheno.csv" % soft_file_input.filename
    phenotype_var.filepath = exp.get_data_file_path(phenotype_var.filename)

    phenotype_var.org = expression_var.org
    phenotype_var.units = expression_var.units

    #ctx[phenotype_var_name] = phenotype_var
    ctx["phenotype"] = phenotype_var
    df2.to_csv(phenotype_var.filepath, sep=phenotype_var.delimiter, index_label=False)

    gmt = GMT()
    gmt.gene_sets = probe_to_genes_mapping
    gmt.description = dict([(key, "") for key in probe_to_genes_mapping])

    gs = GeneSets()
    gs.filename = "%s_gs.gmt" % "gene_sets"
    gs.filepath = exp.get_data_file_path(gs.filename)
    gs.gene_units = "ENTREZ_GENE_ID"
    gs.set_units = "PROBE_ID"
    gmt.write_file(gs.filepath)

    ctx["gene_sets"] = gs

    return ctx

@task(name="workflow.common_tasks.converse_probes_to_genes")
def converse_probes_to_genes(ctx):
    exp = Experiment.objects.get(e_id=ctx["exp_id"])

    #expression_var_name = ctx["expression_var"]
    #expression_trans_var_name = ctx["expression_trans_var"]
    #genesets_var_name = ctx["gene_sets_var"]

    gmt = ctx["gene_sets"].get_gmt()
    set_by_gene = transpose_dict_list(gmt.gene_sets)

    #expression_var = ctx[expression_var_name]
    expression_var = ctx["expression"]
    src = DataFrame.from_csv(expression_var.filepath, sep=" ")

    res = DataFrame(index=set_by_gene.keys(), columns=src.columns)
    for gen, set_ids in set_by_gene.iteritems():
        cut = src.loc[set_ids]
        res.loc[gen] = cut.mean()

    res = res.dropna()

    expression_var_trans = MixData()
    expression_var_trans.org = expression_var.org
    expression_var_trans.units = ["gene_ids"]
    expression_var_trans.filename = "%s_expression_trans.csv" % ctx["dataset_var"]
    expression_var_trans.filepath = exp.get_data_file_path(expression_var_trans.filename)

    res.to_csv(expression_var_trans.filepath, sep=" ", index_label=False)
    #ctx[expression_trans_var_name] = expression_var_trans
    ctx["expression_transformed"] = expression_var_trans
    return ctx

@task(name="workflow.common")
def fetch_msigdb(ctx):
    exp = Experiment.objects.get(e_id=ctx["exp_id"])

    msigdb_gs = GeneSets()
    msigdb_gs.gene_units = "ENTREZ_GENE_ID"
    msigdb_gs.set_units = "gene_sets"

    msigdb_gs.filename = "msigdb.v4.0.entrez.gmt"
    msigdb_gs.filepath = exp.get_data_file_path(msigdb_gs.filename)

    # TODO: be able to choose different db
    url = "http://www.broadinstitute.org/gsea/msigdb/download_file.jsp?filePath=/resources/msigdb/4.0/msigdb.v4.0.entrez.gmt"

    mb_cached = CachedFile.look_up(url)
    if mb_cached is None:
        fetch_file_from_url(url, msigdb_gs.filepath, do_unpuck=False)
        CachedFile.update_cache(url, msigdb_gs.filepath)
    else:
        shutil.copy(mb_cached.get_file_path(), msigdb_gs.filepath)
        print "copied file from cache"

    ctx["gene_sets"] = msigdb_gs
    return ctx


@task(name="workflow.common_tasks.map_gene_sets_to_probes")
def map_gene_sets_to_probes(ctx):
    exp = Experiment.objects.get(e_id=ctx["exp_id"])
    msigdb_gs = ctx["msigdb"]
    assert msigdb_gs.gene_units == "ENTREZ_GENE_ID"

    soft_var_name = ctx["dataset_var"]
    soft_file_input = ctx["input_vars"][soft_var_name]
    soft = list(parse_geo(open(soft_file_input.filepath)))
    pl = soft[2].table_rows
    id_idx = pl[0].index('ID')
    entrez_idx = pl[0].index('ENTREZ_GENE_ID')
    probe_to_genes_mapping = dict([(row[id_idx], row[entrez_idx].split(" /// ")) for row in pl[1:]])

    genes_to_probes = transpose_dict_list(probe_to_genes_mapping)
    genes_to_probes.pop("")
    gs_to_probes = defaultdict(list)

    msigdb_gmt = msigdb_gs.get_gmt()
    for gs, gene_ids in msigdb_gmt.gene_sets.iteritems():
        for gene_id in gene_ids:
            gs_to_probes[gs].extend(genes_to_probes.get(gene_id, []))

    new_gmt = GMT()
    new_gmt.gene_sets = gs_to_probes
    new_gmt.description = msigdb_gmt.description
    new_gmt.units = ["PROBE_ID",]

    new_gs = GeneSets()
    new_gs.filename = "%s_gs.gmt" % "gs_probes_merged"
    new_gs.filepath = exp.get_data_file_path(new_gs.filename)
    new_gs.gene_units = "PROBE_ID"
    new_gs.set_units = "gene_sets"
    new_gmt.write_file(new_gs.filepath)

    ctx["gs_probes_merged"] = new_gs
    return ctx


@task(name="workflow.common_tasks.gt_pval_cut")
def gt_pval_cut(ctx):
    exp = Experiment.objects.get(e_id=ctx["exp_id"])

    df = DataFrame.from_csv(ctx['mgt_result'].filepath, sep=' ')

    cut_val = ctx["input_vars"]["common_settings"].inputs["pval_cut"].value
    index = df[df['p-value'] <= cut_val].index

    # filter expression_train and expression_test
    expression_train = ctx["expression_train"]
    train_df = expression_train.to_data_frame()
    train_df = train_df.loc[index]
    train_df.to_csv(expression_train.filepath, sep=expression_train.delimiter, index_label=False)

    expression_test = ctx["expression_test"]
    test_df = expression_test.to_data_frame()
    test_df = test_df.loc[index]
    test_df.to_csv(expression_test.filepath, sep=expression_test.delimiter, index_label=False)

    return ctx

