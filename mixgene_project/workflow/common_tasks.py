import shutil
import gzip
from celery import task
from pandas import Series, DataFrame

from sklearn import cross_validation

from Bio.Geo import parse as parse_geo

from mixgene.util import prepare_GEO_ftp_url, fetch_file_from_url
from webapp.models import CachedFile, Experiment
from environment.units import GeneUnits
from environment.structures import ExpressionSet, PlatformAnnotation, \
    GeneSets, GS, FileInputVar
import sys
import traceback
from webapp.scope import ScopeRunner


@task(name="workflow.common_tasks.fetch_GEO_gse")
def fetch_geo_gse(exp, block,
                  geo_uid,
                  success_action="success", error_action="error",
                  ignore_cache=False):
    """
        @type  exp: webapp.models.Experiment

        @type  block: GenericBlock


        Fetch dataset from GEO and caching it.
    """
    # noinspection PyBroadException
    try:
        file_format = "soft"
        geo_uid = geo_uid.upper()

        url, compressed_filename, filename = prepare_GEO_ftp_url(geo_uid, file_format)

        fi = FileInputVar("%s/%s_source.soft.gz" % (exp.get_data_folder(), block.uuid), title="", description="")
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

        #print "current block state: ", exp.state
        block.do_action(success_action, exp, fi)

    except RuntimeError, e:
        import sys, traceback
        ex_type, ex, tb = sys.exc_info()
        traceback.print_tb(tb)
        #print e
        block.errors.append(e)
        block.do_action(error_action, exp)


@task(name="workflow.common_tasks.preprocess_soft")
def preprocess_soft(exp, block,
        source_file,
        success_action="success", error_action="error"
    ):
    """
        @type source_file: FileInputVar
        Produce symbols and phenotype objects( MixData, MixPheno) from .soft file.
    """
    try:
        #TODO: now we assume that we get GSE file
        try:
            soft = list(parse_geo(gzip.open(source_file.filepath)))
        except:
            raise RuntimeError("Bad source file, can't read")

        assert soft[2].entity_type == "PLATFORM"

        pl = soft[2].table_rows
        id_idx = pl[0].index('ID')
        entrez_idx = pl[0].index('ENTREZ_GENE_ID')

        #TODO bug here
        probe_to_genes_GS = GS()
        for row in pl[1:]:
            probe_to_genes_GS.description[row[id_idx]] = ""
            probe_to_genes_GS.genes[row[id_idx]] = row[entrez_idx].split(" /// ")

        platform_annotation = PlatformAnnotation("TODO:GET NAME FROM SOFT",
            base_dir=exp.get_data_folder(),
            base_filename="%s_annotation" % block.uuid
        )

        platform_annotation.gene_sets.metadata["gene_units"] = GeneUnits.ENTREZ_ID
        platform_annotation.gene_sets.metadata["set_units"] = GeneUnits.PROBE_ID
        platform_annotation.gene_sets.store_gs(probe_to_genes_GS)

        id_ref_idx = soft[3].table_rows[0].index("ID_REF")
        value_idx = soft[3].table_rows[0].index("VALUE")
        assay_df = DataFrame(dict([
            (
                soft[i].entity_attributes['Sample_geo_accession'],
                Series(dict([(row[id_ref_idx], row[value_idx]) for row in soft[i].table_rows[1:]]))
            )
            for i in range(3, len(soft))
        ]))

        expression_set = ExpressionSet(exp.get_data_folder(), "%s_es" % block.uuid)
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

        block.do_action(success_action, exp,
                        expression_set, platform_annotation)
    except Exception, e:

        ex_type, ex, tb = sys.exc_info()
        traceback.print_tb(tb)
        print e
        #TODO: LOG ERROR AND TRACEBACK OR WE LOSE IT!
        #import ipdb; ipdb.set_trace()
        block.errors.append(e)
        block.do_action(error_action, exp)


@task(name="workflow.common_tasks.generate_cv_folds")
def generate_cv_folds(exp, block,
                      folds_num,
                      # split_ratio,
                      es):
    """
        On success populate block.sequence with correct folds and
         call action #on_generate_folds_done otherwise calls
                     #on_generate_folds_error
        @type es: ExpressionSet
    """
    try:

        #print folds_num
        #print split_ratio

        assay_df = es.get_assay_data_frame()
        pheno_df = es.get_pheno_data_frame()

        #TODO: fix in block parser
        # split_ratio = float(split_ratio)
        folds_num = int(folds_num)

        if "User_class" not in pheno_df.columns:
            raise RuntimeError("Phenotype doesn't have user assigned classes")

        classes_vector = pheno_df["User_class"].values
        i = 0
        # for train_idx, test_idx in cross_validation.StratifiedShuffleSplit(
        #         classes_vector,
        #         n_iter=folds_num,
        #         train_size=split_ratio, test_size= 1- split_ratio):
        #
        for train_idx, test_idx in cross_validation.StratifiedKFold(
            classes_vector,
            n_iter=folds_num
        ):


            train_es = es.clone( "%s_train_%s" % (es.base_filename ,i))
            train_es.store_assay_data_frame(assay_df[train_idx])
            train_es.store_pheno_data_frame(pheno_df.iloc[train_idx])

            test_es = es.clone("%s_test_%s" % (es.base_filename, i))
            test_es.store_assay_data_frame(assay_df[test_idx])
            test_es.store_pheno_data_frame(pheno_df.iloc[test_idx])

            block.sequence.append({
                "es_train_i": train_es,
                "es_test_i": test_es
            })

            i += 1

        block.do_action("on_generate_folds_done", exp)
    except Exception, e:

        ex_type, ex, tb = sys.exc_info()
        traceback.print_tb(tb)
        print e
        #TODO: LOG ERROR AND TRACEBACK OR WE LOSE IT!
        block.errors.append(e)
        block.do_action("on_generate_folds_error", exp)


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
