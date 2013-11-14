from collections import defaultdict
from pprint import pprint
import shutil
import random
import gzip
from celery import task
from pandas import Series, DataFrame

from Bio.Geo import parse as parse_geo

from mixgene.util import prepare_GEO_ftp_url, fetch_file_from_url, clean_GEO_file, transpose_dict_list
from webapp.models import Experiment, CachedFile
from workflow.constants import Units
from workflow.input import FileInputVar
from workflow.parsers import GMT
from workflow.structures import ExpressionSet, PlatformAnnotation
from workflow.vars import MixData, MixPheno, GeneSets


@task(name="workflow.common_tasks.append_error_to_block")
def append_error_to_block(*args, **kwargs):
    #block.errors.appen
    pprint(args)
    pprint(kwargs)
    print("==" * 25)


@task(name="workflow.common_tasks.fetch_GEO_gse_matrix")
def fetch_geo_gse(exp, block, ignore_cache=False):
    """
        @type  exp: webapp.models.Experiment
        @param exp:

        @type  block:
        @param block:

        Fetch dataset from GEO and caching it.
    """
    # noinspection PyBroadException
    try:
        file_format = "soft"
        geo_uid = block.get_geo_uid().upper()

        url, compressed_filename, filename = prepare_GEO_ftp_url(geo_uid, file_format)

        fi = FileInputVar(block.get_gse_source_name(), title="", description="")
        fi.is_done = True
        fi.is_being_fetched = False
        fi.file_extension = "soft.gz"
        fi.is_gzipped = True
        fi.filename = filename

        fi.filepath = exp.get_data_file_path(fi.filename, fi.file_extension)
        fi.geo_uid = geo_uid
        fi.geo_type = geo_uid[:3]

        fi.file_format = file_format
        fi.set_file_type("ncbi_geo")

        mb_cached = CachedFile.look_up(url)
        if mb_cached is None or ignore_cache:
            #FIME: grrrrrr...
            dir_path = exp.get_data_folder()
            fetch_file_from_url(url, "%s/%s" % (dir_path, compressed_filename))

            CachedFile.update_cache(url, fi.filepath)
        else:
            shutil.copy(mb_cached.get_file_path(), fi.filepath)
            print "copied file from cache"

        block.source_file = fi
        block.do_action("successful_fetch", exp)

    except RuntimeError, e:
        print e
        block.errors.append(e)
        block.do_action("error_during_fetch", exp)


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
def preprocess_soft(exp, block):
    """
        Produce symbols and phenotype objects( MixData, MixPheno) from .soft file.
    """
    try:
        #TODO: now we assume that we get GSE file
        try:
            soft = list(parse_geo(gzip.open(block.source_file.filepath)))
        except:
            raise RuntimeError("Bad source file, can't read")

        assert soft[2].entity_type == "PLATFORM"

        pl = soft[2].table_rows
        id_idx = pl[0].index('ID')
        entrez_idx = pl[0].index('ENTREZ_GENE_ID')
        probe_to_genes_mapping = dict([
            (
                row[id_idx],
                ("", row[entrez_idx].split(" /// "))
            )
            for row in pl[1:]
        ])

        platform_annotation = PlatformAnnotation("TODO:GET NAME FROM SOFT",
             base_dir=exp.get_data_folder(),
             base_filename=block.uuid + "_annotation"
        )

        platform_annotation.gene_units = Units.ENTREZ_GENE_ID
        platform_annotation.set_units = Units.PROBE_ID
        platform_annotation.store_gmt(probe_to_genes_mapping)
        block.gpl_annotation = platform_annotation

        id_ref_idx = soft[3].table_rows[0].index("ID_REF")
        value_idx = soft[3].table_rows[0].index("VALUE")
        assay_df = DataFrame(dict([
            (
                soft[i].entity_attributes['Sample_geo_accession'],
                Series(dict([(row[id_ref_idx], row[value_idx]) for row in soft[i].table_rows[1:]]))
            )
            for i in range(3, len(soft))
        ]))

        expression_set = ExpressionSet(exp.get_data_folder(), block.uuid + "_es")
        expression_set.store_assay_data_frame(assay_df)

        factors = [soft[i].entity_attributes
                   for i in range(3, len(soft))]
        pheno_index = []
        for factor in factors:
            factor.pop('sample_table_begin')
            factor.pop('sample_table_end')
            pheno_index.append(factor.pop('Sample_geo_accession'))

        pheno_df = DataFrame([Series(factor) for factor in factors], index=pheno_index)
        pheno_df.index.name = 'Sample_geo_accession'
        expression_set.store_pheno_data_frame(pheno_df)
        block.expression_set = expression_set

        block.do_action("successful_preprocess", exp)
    except Exception, e:
        print e
        #TODO: LOG ERROR AND TRACEBACK OR WE LOSE IT!
        #import ipdb; ipdb.set_trace()
        block.errors.append(e)
        block.do_action("error_during_preprocess", exp)


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
    # FIXME: now this is a fake url sine broadinsitute require auth
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
    soft = list(parse_geo(gzip.open(soft_file_input.filepath)))
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

